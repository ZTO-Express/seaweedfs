package util

// Semaphore 结构体实现信号量
type Semaphore struct {
	tokens chan struct{}
}

// NewSemaphore 创建新的信号量
func NewSemaphore(max int) *Semaphore {
	return &Semaphore{
		tokens: make(chan struct{}, max), // 创建带缓冲的通道
	}
}

// Acquire 请求信号量
func (s *Semaphore) Acquire() {
	s.tokens <- struct{}{} // 将空结构体放入通道，表示占用一个资源
}

// Release 释放信号量
func (s *Semaphore) Release() {
	<-s.tokens // 从通道中取出一个空结构体，表示释放一个资源
}
