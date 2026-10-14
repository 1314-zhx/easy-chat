package bitmap

type Bitmap struct {
	bits []byte
	size int
}

func NewBitmap(size int) *Bitmap {
	// 如果为零，给一个默认值
	if size == 0 {
		size = 250
	}
	// 一个 byte有8个 bit，size是还有多少个bit
	// [0,0,0,0,0,0,0,0],[0,0,0,0,0,0,0,0]
	return &Bitmap{
		bits: make([]byte, size),
		size: size * 8,
	}
}

// 根据Id计算在bitmap中的位置
func (b *Bitmap) Set(id string) {
	// id 在哪个bit 里面
	idx := hash(id) % b.size
	// 计算在哪个 byte
	byteIdx := idx / 8
	// 在这个字节的哪个 bit位置
	bitIdx := idx % 8
	// 已经确定在哪个 byte 了，并且也确定了在该byte 的具体 bit，先在1右边添加 bitIdx个0，再通过 | 运算进行设置。将要设置的位置设置为1，原本为1的位置还是1
	b.bits[byteIdx] |= 1 << bitIdx
}
func (b *Bitmap) IsSet(id string) bool {
	// id 在哪个bit 里面
	idx := hash(id) % b.size
	// 计算在哪个 byte
	byteIdx := idx / 8
	// 在这个字节的哪个 bit位置
	bitIdx := idx % 8
	// 假设 bitIdx 是 3，那么 1 << 3 的结果就是二进制的 1000。
	// 如果该位原本是 1，结果就是 1000（非 0）。
	// 如果该位原本是 0，结果就是 0000（0）。
	return (b.bits[byteIdx] & (1 << bitIdx)) != 0
}

// 导出字节
func (b *Bitmap) Export() []byte {
	return b.bits
}

// 导入，加载给对象
func Load(bits []byte) *Bitmap {
	if len(bits) == 0 {
		return NewBitmap(0)
	}
	return &Bitmap{
		bits: bits,
		size: len(bits) * 8,
	}
}

// 将 string 转化为可以计算的 int
func hash(id string) int {
	// 使用BKDR哈希算法
	seed := 131313 // 31 131 1313 13131 131313, etc
	hash := 0
	for _, c := range id {
		hash = hash*seed + int(c)
	}
	// 0x7FFFFFFF 是32位正整数的天花板，通常作为掩码
	// 在hash运算时，可能会超过int的上限变成负数，与掩码与运算后，切掉超过32位限制的部分，留下一个正数int
	return hash & 0x7FFFFFFF
}
