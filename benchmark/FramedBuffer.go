package benchmark

// 比bufio实现简单，去掉调用以及各种判断
type FrameBuffer struct {
	// 前4个字节预留写长度；这里只用一个buf；另一种做法是多个buf数组拼接不用memmove，但是write会多次，write会陷入内核；
	// 所以这里初次buf长度尽量预估好
	b         []byte
	rwIdx     int 	// read/write-offset, reset when Flush
	frameSize int	// frame size
}

