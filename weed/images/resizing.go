package images

import (
	"bytes"
	"image"
	"image/gif"
	"image/jpeg"
	"image/png"
	"io"
	"runtime"
	"sync"
	"time"

	"github.com/cognusion/imaging"

	"github.com/seaweedfs/seaweedfs/weed/glog"

	_ "golang.org/x/image/webp"
)

// 添加限流器相关代码
var (
	// 默认限制为CPU核心数的一半
	resizeThrottler     = NewThrottler(runtime.NumCPU() / 2)
	resizeThrottlerLock sync.RWMutex
)

// Throttler 用于限制并发操作数量
type Throttler struct {
	tokens chan struct{}
}

// NewThrottler 创建一个新的限流器
func NewThrottler(maxConcurrent int) *Throttler {
	return &Throttler{
		tokens: make(chan struct{}, maxConcurrent),
	}
}

// SetResizeThrottleLimit 设置图像处理的最大并发数
func SetResizeThrottleLimit(limit int) {
	resizeThrottlerLock.Lock()
	defer resizeThrottlerLock.Unlock()

	// 创建新的限流器
	resizeThrottler = NewThrottler(limit)
}

// Acquire 获取一个令牌
func (t *Throttler) Acquire() {
	// 添加日志，记录尝试获取令牌前的通道状态
	if len(t.tokens) >= cap(t.tokens) {
		glog.V(0).Infof("图像处理限流：当前并发数已达上限 %d，请求将被阻塞", cap(t.tokens))
	}
	t.tokens <- struct{}{}
}

// Release 释放一个令牌
func (t *Throttler) Release() {
	<-t.tokens
	// 可以选择在释放令牌时也添加日志
	glog.V(4).Infof("图像处理限流：释放一个令牌，当前并发数 %d/%d", len(t.tokens), cap(t.tokens))
}

func Resized(ext string, read io.ReadSeeker, width, height int, mode string) (resized io.ReadSeeker, w int, h int) {
	// 获取限流令牌
	resizeThrottlerLock.RLock()
	throttler := resizeThrottler
	resizeThrottlerLock.RUnlock()

	// 记录开始尝试获取令牌的时间
	startTime := time.Now()
	throttler.Acquire()
	// 计算获取令牌所花费的时间
	waitTime := time.Since(startTime)
	if waitTime > time.Millisecond*100 {
		// 如果等待时间超过100毫秒，记录一条警告日志
		glog.V(0).Infof("图像处理限流：请求等待时间较长 %v，处理图像 %dx%d，模式 %s", waitTime, width, height, mode)
	} else {
		// 记录常规日志
		glog.V(2).Infof("图像处理：开始处理图像 %dx%d，模式 %s", width, height, mode)
	}
	defer throttler.Release()

	if width == 0 && height == 0 {
		return read, 0, 0
	}
	srcImage, _, err := image.Decode(read)
	if err == nil {
		bounds := srcImage.Bounds()
		var dstImage *image.NRGBA
		if bounds.Dx() > width && width != 0 || bounds.Dy() > height && height != 0 {
			switch mode {
			case "fit":
				dstImage = imaging.Fit(srcImage, width, height, imaging.Lanczos)
			case "fill":
				dstImage = imaging.Fill(srcImage, width, height, imaging.Center, imaging.Lanczos)
			default:
				if width == height && bounds.Dx() != bounds.Dy() {
					dstImage = imaging.Thumbnail(srcImage, width, height, imaging.Lanczos)
					w, h = width, height
				} else {
					dstImage = imaging.Resize(srcImage, width, height, imaging.Lanczos)
				}
			}
		} else {
			read.Seek(0, 0)
			return read, bounds.Dx(), bounds.Dy()
		}
		var buf bytes.Buffer
		switch ext {
		case ".png":
			png.Encode(&buf, dstImage)
		case ".jpg", ".jpeg":
			jpeg.Encode(&buf, dstImage, nil)
		case ".gif":
			gif.Encode(&buf, dstImage, nil)
		case ".webp":
			// Webp does not have golang encoder.
			png.Encode(&buf, dstImage)
		}
		return bytes.NewReader(buf.Bytes()), dstImage.Bounds().Dx(), dstImage.Bounds().Dy()
	} else {
		glog.Error(err)
	}
	return read, 0, 0
}
