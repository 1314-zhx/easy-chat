/**
 * @author: dn-jinmin/dn-jinmin
 * @doc:
 */

package websocket

import "net/http"

type DailOptions func(option *dailOption)

type dailOption struct {
	pattern string      // 请求ws 的地址
	header  http.Header // 请求头
}

func newDailOptions(opts ...DailOptions) dailOption {
	o := dailOption{
		pattern: "/ws",
		header:  nil,
	}

	for _, opt := range opts {
		opt(&o)
	}

	return o
}
func WithClientPatten(pattern string) DailOptions {
	return func(opt *dailOption) {
		opt.pattern = pattern
	}
}
func WithClientHeader(header http.Header) DailOptions {
	return func(opt *dailOption) {
		opt.header = header
	}
}
