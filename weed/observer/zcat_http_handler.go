package observer

import (
	"fmt"
	"git.ztosys.com/ZTO_CS/cat-go/cat"
	middleware_http "git.ztosys.com/ZTO_CS/zcat-go-sdk/middleware/http"
	"git.ztosys.com/ZTO_CS/zcat-go-sdk/zcat"
	"net/http"
)

func ZcatHandlerFunc(role string) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if !cat.IsEnabled() {
				next.ServeHTTP(w, req)
				return
			}

			spanContext := middleware_http.ExtractSpanContext(req.Header)
			if spanContext.Empty() {
				spanContext = zcat.ChildOfSpanContext(spanContext)
			}

			volumeIgnorePath := role == RoleVolume //volume http请求忽略path部分，避免埋点量过大
			var path string
			if volumeIgnorePath {
				path = "{volume}"
			} else {
				path = req.URL.Path
			}
			transactor := zcat.StartTransactorWithSpanContext(spanContext, zcat.HttpServer.String(), fmt.Sprintf("%s_%s", path, req.Method))
			if volumeIgnorePath {
				transactor.LogEvent(RoleVolume, "HttpRequestPath", "", req.URL.Path)
			}
			defer transactor.Complete()
			ctx := zcat.ContextWithTransactor(req.Context(), transactor)
			next.ServeHTTP(w, req.WithContext(ctx))
		})
	}
}
