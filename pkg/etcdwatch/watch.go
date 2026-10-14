package etcdwatch

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type Watcher struct {
	cli *clientv3.Client
}

func New(endpoints []string) (*Watcher, error) {
	cli, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 5 * time.Second})
	if err != nil {
		return nil, err
	}
	return &Watcher{cli: cli}, nil
}

func (w *Watcher) Close() error {
	if w.cli == nil {
		return nil
	}
	return w.cli.Close()
}

func (w *Watcher) Get(ctx context.Context, key string) ([]byte, error) {
	resp, err := w.cli.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, nil
	}
	return resp.Kvs[0].Value, nil
}

func (w *Watcher) Put(ctx context.Context, key string, val []byte) error {
	_, err := w.cli.Put(ctx, key, string(val))
	return err
}

// Watch 监听单 key 的 PUT / DELETE。回调出错只打日志，不中断监听。
func (w *Watcher) Watch(ctx context.Context, key string, onPut func([]byte), onDelete func()) error {
	rch := w.cli.Watch(ctx, key)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case resp, ok := <-rch:
			if !ok {
				return fmt.Errorf("watch closed")
			}
			for _, ev := range resp.Events {
				switch ev.Type {
				case clientv3.EventTypeDelete:
					if onDelete != nil {
						onDelete()
					}
				default:
					if ev.Kv == nil || onPut == nil {
						continue
					}
					onPut(ev.Kv.Value)
				}
			}
		}
	}
}

// SyncAndWatch 把本地配置写入 etcd（key 已存在则用 etcd 里的），失败则继续用本地；然后 Watch PUT/删除。
func SyncAndWatch(ctx context.Context, endpoints []string, key string, localJSON []byte, onPut func([]byte), onDelete func()) {
	if len(endpoints) == 0 {
		fmt.Println("etcd config: no endpoints, use local file")
		return
	}
	w, err := New(endpoints)
	if err != nil {
		fmt.Printf("etcd config: connect failed, use local file: %v\n", err)
		return
	}

	go func() {
		defer w.Close()

		existing, err := w.Get(ctx, key)
		if err != nil {
			fmt.Printf("etcd config: get failed, try put local: %v\n", err)
			if putErr := w.Put(ctx, key, localJSON); putErr != nil {
				fmt.Printf("etcd config: put failed, stay on local file: %v\n", putErr)
			} else {
				fmt.Printf("etcd config: put local file to %s\n", key)
			}
		} else if len(existing) > 0 {
			fmt.Printf("etcd config: loaded existing %s\n", key)
			if onPut != nil {
				onPut(existing)
			}
		} else if putErr := w.Put(ctx, key, localJSON); putErr != nil {
			fmt.Printf("etcd config: put failed, stay on local file: %v\n", putErr)
		} else {
			fmt.Printf("etcd config: put local file to %s\n", key)
		}

		if err := w.Watch(ctx, key, onPut, onDelete); err != nil && ctx.Err() == nil {
			fmt.Printf("etcd config: watch stopped: %v\n", err)
		}
	}()
}

func Encode(v any) ([]byte, error) { return json.Marshal(v) }
func Decode(b []byte, v any) error { return json.Unmarshal(b, v) }
