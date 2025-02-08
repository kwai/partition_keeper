package utils

import (
	"net/http"
	"net/http/pprof"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
)

func RegisterHandler(
	mux *runtime.ServeMux,
	method, path string,
	handler http.Handler,
) {
	err := mux.HandlePath(
		method,
		path,
		func(w http.ResponseWriter, r *http.Request, pathParams map[string]string) {
			handler.ServeHTTP(w, r)
		},
	)
	if err != nil {
		logging.Fatal("register handler %s failed: %s", path, err.Error())
	}
}

func RegisterHandlerFunc(
	mux *runtime.ServeMux,
	method, path string,
	handler http.HandlerFunc,
) {
	err := mux.HandlePath(
		method,
		path,
		func(w http.ResponseWriter, r *http.Request, pathParams map[string]string) {
			handler(w, r)
		},
	)
	if err != nil {
		logging.Fatal("register handler %s failed: %s", path, err.Error())
	}
}

func RegisterKcsPath(mux *runtime.ServeMux) {
	RegisterHandlerFunc(
		mux,
		http.MethodGet,
		"/health",
		func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("ok"))
		},
	)
}

func RegisterProfPath(mux *runtime.ServeMux) {
	RegisterHandlerFunc(mux, http.MethodGet, "/debug/pprof", pprof.Index)

	RegisterHandlerFunc(mux, http.MethodGet, "/debug/cmdline", pprof.Cmdline)
	RegisterHandlerFunc(mux, http.MethodGet, "/debug/profile", pprof.Profile)
	RegisterHandlerFunc(mux, http.MethodPost, "/debug/symbol", pprof.Symbol)
	RegisterHandlerFunc(mux, http.MethodGet, "/debug/symbol", pprof.Symbol)
	RegisterHandlerFunc(mux, http.MethodGet, "/debug/trace", pprof.Trace)

	RegisterHandler(mux, http.MethodGet, "/debug/allocs", pprof.Handler("allocs"))
	RegisterHandler(mux, http.MethodGet, "/debug/block", pprof.Handler("block"))
	RegisterHandler(mux, http.MethodGet, "/debug/goroutine", pprof.Handler("goroutine"))
	RegisterHandler(mux, http.MethodGet, "/debug/heap", pprof.Handler("heap"))
	RegisterHandler(mux, http.MethodGet, "/debug/mutex", pprof.Handler("mutex"))
	RegisterHandler(mux, http.MethodGet, "/debug/threadcreate", pprof.Handler("threadcreate"))
}
