package benchmark

// 比bufio实现简单，去掉调用以及各种判断
type BinaryBuffer struct {

	b         []byte
	wIdx     int 	// write-offset, read data from socket, then write to buf
	rIdx     int 	// read-offset, read data from buf to outer use
	rLen     int 	// total read size from socket
	err      error	// 读错误
}
