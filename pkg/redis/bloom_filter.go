package redis

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

// BloomFilter 布隆过滤器 - 防止缓存穿透
type BloomFilter struct {
	client *redis.Redis
	key    string // Redis key 用于存储 bloom filter 的位数组
	size   uint64 // 位数组大小
	hashes uint   // 哈希函数数量
}

// NewBloomFilter 创建布隆过滤器
// key: Redis 中存储位数组的 key
// size: 位数组大小（位数）
// hashes: 哈希函数数量
// 数组长度（ m ）和预计插入的元素数量（ n ）。
// 最佳哈希函数数量：m / n * ln2
// 因为 ln2 约等于0.693，所以一般是每个元素分配的平均 Bit 数 乘以 0.7
func NewBloomFilter(client *redis.Redis, key string, size uint64, hashes uint) *BloomFilter {
	if size == 0 {
		size = 1 << 20 // 默认 1M 位，约 128KB
	}
	if hashes == 0 {
		// 最佳哈希函数数量：m / n * ln2
		// 当 size=1M 时，hashes ≈ 7
		hashes = 7
	}
	return &BloomFilter{
		client: client,
		key:    key,
		size:   size,
		hashes: hashes,
	}
}

// keyName 生成 Redis key
func (b *BloomFilter) keyName() string {
	return fmt.Sprintf("bloom:%s", b.key)
}

// hash 计算元素的多个哈希值
func (b *BloomFilter) hash(item string) []uint64 {
	h1 := b.hash1(item)
	h2 := b.hash2(item)

	hashes := make([]uint64, b.hashes)
	for i := uint(0); i < b.hashes; i++ {
		hashes[i] = (h1 + uint64(i)*h2) % b.size
	}
	return hashes
}

// hash1 第一个哈希函数
func (b *BloomFilter) hash1(item string) uint64 {
	h := sha256.New()
	h.Write([]byte(item))
	return binary.BigEndian.Uint64(h.Sum(nil)[:8])
}

// hash2 第二个哈希函数
func (b *BloomFilter) hash2(item string) uint64 {
	h := sha256.New()
	h.Write([]byte(item + "salt")) // 添加盐值
	return binary.BigEndian.Uint64(h.Sum(nil)[:8])
}

// Add 添加元素到布隆过滤器
func (b *BloomFilter) Add(item string) error {
	hashes := b.hash(item)
	for _, h := range hashes {
		if _, err := b.client.SetBit(b.keyName(), int64(h), 1); err != nil {
			return err
		}
	}
	return nil
}

// Exists 检查元素是否可能存在
// 返回 true：可能存在（不一定是真阳性）
// 返回 false：一定不存在
func (b *BloomFilter) Exists(item string) (bool, error) {
	hashes := b.hash(item)
	for _, h := range hashes {
		val, err := b.client.GetBit(b.keyName(), int64(h))
		if err != nil {
			return false, err
		}
		if val == 0 {
			return false, nil // 一定不存在
		}
	}
	return true, nil // 可能存在
}

// MayExist 检查元素是否可能存在（别名）
func (b *BloomFilter) MayExist(item string) (bool, error) {
	return b.Exists(item)
}

// MustNotExist 确认元素一定不存在
func (b *BloomFilter) MustNotExist(item string) (bool, error) {
	exists, err := b.Exists(item)
	if err != nil {
		return false, err
	}
	return !exists, nil
}

// Reset 重置布隆过滤器
func (b *BloomFilter) Reset() error {
	_, err := b.client.Del(b.keyName())
	return err
}

// WithCacheAside 旁路缓存 + 布隆过滤器的组合
// 返回值：mayExist - 布隆过滤器判断元素是否可能存在
func (b *BloomFilter) CheckBeforeCache(query string) (bool, error) {
	return b.Exists(query)
}
