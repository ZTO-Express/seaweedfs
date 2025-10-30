package observer

import (
	"fmt"
	"git.ztosys.com/ZTO_CS/cat-go/cat"
	middleware_http "git.ztosys.com/ZTO_CS/zcat-go-sdk/middleware/http"
	"git.ztosys.com/ZTO_CS/zcat-go-sdk/zcat"
	"net/http"
)

var ZcatHandlerFunc = func(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if !cat.IsEnabled() {
			next.ServeHTTP(w, req)
			return
		}

		spanContext := middleware_http.ExtractSpanContext(req.Header)
		if spanContext.Empty() {
			spanContext = zcat.ChildOfSpanContext(spanContext)
		}
		transactor := zcat.StartTransactorWithSpanContext(spanContext, zcat.HttpServer.String(), fmt.Sprintf("%s_%s", req.URL.Path, req.Method))
		defer transactor.Complete()
		ctx := zcat.ContextWithTransactor(req.Context(), transactor)
		next.ServeHTTP(w, req.WithContext(ctx))
	})
}
