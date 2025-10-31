package observer

import (
	"context"
	"git.ztosys.com/ZTO_CS/cat-go/cat"
	middleware_http "git.ztosys.com/ZTO_CS/zcat-go-sdk/middleware/http"
	"git.ztosys.com/ZTO_CS/zcat-go-sdk/zcat"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// ZcatUnaryClientInterceptor returns a grpc.UnaryClientInterceptor that takes values stored in the
// context (using WithOutgoingValue) for the provided keys and injects them into the outgoing
// gRPC metadata before performing the RPC.
var ZcatUnaryClientInterceptor = func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	if !cat.IsEnabled() {
		return invoker(ctx, method, req, reply, cc, opts...)
	}

	var spanCtx zcat.SpanContext
	transactor, ok := zcat.TransactorFromContext(ctx)
	if ok {
		spanCtx = transactor.SpanContext()
	} else {
		spanCtx = zcat.ChildOfSpanContext(zcat.EmptySpanContext)
		transactor = zcat.StartTransactorWithSpanContext(spanCtx, zcat.GrpcClient.String(), method)
		defer transactor.Complete()
	}
	childId := zcat.GenerateId()
	transactor.LogEvent(zcat.RemoteCall.String(), "", "0", childId)

	// inject metadata
	md := metadata.New(nil)
	md.Append(middleware_http.RootMessageID, spanCtx.RootMessageID)
	md.Append(middleware_http.ParentMessageID, spanCtx.ParentMessageID)
	md.Append(middleware_http.ChildMessageID, childId)

	ctx = metadata.NewOutgoingContext(ctx, md)
	return invoker(ctx, method, req, reply, cc, opts...)

}

// ZcatUnaryServerInterceptor returns a grpc.UnaryServerInterceptor that extracts selected metadata
// from incoming requests and stores them into the request context so handlers and downstream
// logic can access them via GetOutgoingValue with the same keys.
var ZcatUnaryServerInterceptor = func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	if !cat.IsEnabled() {
		return handler(ctx, req)
	}

	meta, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return handler(ctx, req)
	}

	spanContext := extractSpanContext(meta)
	if spanContext.Empty() {
		spanContext = zcat.ChildOfSpanContext(spanContext)
	}
	trans := zcat.StartTransactorWithSpanContext(spanContext, zcat.GrpcServer.String(), info.FullMethod)
	defer trans.Complete()

	//trans.LogEvent(zcat.GrpcServer.String(), "", "0", childSpanContext.ChildMessageID)
	ctx = zcat.ContextWithTransactor(ctx, trans)
	resp, err := handler(ctx, req)
	if err != nil {
		trans.SetStatus(cat.FAIL)
	}
	return resp, err
}

// ZcatStreamClientInterceptor returns a grpc.StreamClientInterceptor that injects selected
// context values into outgoing metadata for streaming RPCs.
var ZcatStreamClientInterceptor = func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	transactor, ok := zcat.TransactorFromContext(ctx)
	if !ok {
		return streamer(ctx, desc, cc, method, opts...)
	}

	spanCtx := transactor.SpanContext()
	childId := zcat.GenerateId()
	md := metadata.New(nil)
	// collect user-supplied values
	md.Append(middleware_http.RootMessageID, spanCtx.RootMessageID)
	md.Append(middleware_http.ParentMessageID, spanCtx.ParentMessageID)
	md.Append(middleware_http.ChildMessageID, childId)
	transactor.LogEvent(zcat.GrpcClient.String(), "", "0", childId)

	ctx = metadata.NewOutgoingContext(ctx, md)
	return streamer(ctx, desc, cc, method, opts...)
}

// ZcatStreamServerInterceptor returns a grpc.StreamServerInterceptor that extracts selected metadata
// from incoming streaming RPCs and stores them into the stream context.
var ZcatStreamServerInterceptor = func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	var ctx = ss.Context()

	var ok bool
	var meta metadata.MD
	if meta, ok = metadata.FromIncomingContext(ctx); !ok || !cat.IsEnabled() {
		return handler(srv, ss)
	}

	spanContext := extractSpanContext(meta)
	if spanContext.Empty() {
		spanContext = zcat.ChildOfSpanContext(spanContext)
	}
	trans := zcat.StartTransactorWithSpanContext(spanContext, zcat.GrpcServer.String(), info.FullMethod)
	defer trans.Complete()

	//trans.LogEvent(zcat.GrpcServer.String(), "", "0", childSpanContext.ChildMessageID)
	ctx = zcat.ContextWithTransactor(ctx, trans)
	wss := &wrappedServerStream{ServerStream: ss, ctx: ctx}
	err := handler(srv, wss)
	if err != nil {
		trans.LogEvent(zcat.RuntimeException.String(), "sys_error", err.Error())
		trans.SetStatus(cat.FAIL)
	}
	return err
}

func extractSpanContext(md metadata.MD) zcat.SpanContext {
	if len(md) == 0 {
		return zcat.EmptySpanContext
	}
	root := md.Get(middleware_http.RootMessageID)
	parent := md.Get(middleware_http.ParentMessageID)
	child := md.Get(middleware_http.ChildMessageID)
	if len(root) == 1 && len(parent) == 1 && len(child) == 1 {
		return zcat.SpanContext{
			RootMessageID:   root[0],
			ParentMessageID: parent[0],
			ChildMessageID:  child[0],
		}
	}
	return zcat.EmptySpanContext
}

// wrappedServerStream is used to override the Context() method of grpc.ServerStream
// so we can inject values into the stream context before the handler is invoked.
type wrappedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedServerStream) Context() context.Context { return w.ctx }

func (w *wrappedServerStream) RecvMsg(m interface{}) error {
	err := w.ServerStream.RecvMsg(m)
	if err == nil {
		//fmt.Printf("[trace:%s] Recv message: %+v\n", w.traceID, m)
	}
	return err
}

func (w *wrappedServerStream) SendMsg(m interface{}) error {
	return w.ServerStream.SendMsg(m)
}
