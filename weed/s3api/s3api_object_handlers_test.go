package s3api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/stretchr/testify/assert"
)

func TestRemoveDuplicateSlashes(t *testing.T) {
	tests := []struct {
		name           string
		path           string
		expectedResult string
	}{
		{
			name:           "empty",
			path:           "",
			expectedResult: "",
		},
		{
			name:           "slash",
			path:           "/",
			expectedResult: "/",
		},
		{
			name:           "object",
			path:           "object",
			expectedResult: "object",
		},
		{
			name:           "correct path",
			path:           "/path/to/object",
			expectedResult: "/path/to/object",
		},
		{
			name:           "path with duplicates",
			path:           "///path//to/object//",
			expectedResult: "/path/to/object/",
		},
	}

	for _, tst := range tests {
		t.Run(tst.name, func(t *testing.T) {
			obj := removeDuplicateSlashes(tst.path)
			assert.Equal(t, tst.expectedResult, obj)
		})
	}
}

func TestS3ApiServer_toFilerUrl(t *testing.T) {
	tests := []struct {
		name string
		args string
		want string
	}{
		{
			"simple",
			"/uploads/eaf10b3b-3b3a-4dcd-92a7-edf2a512276e/67b8b9bf-7cca-4cb6-9b34-22fcb4d6e27d/Bildschirmfoto 2022-09-19 um 21.38.37.png",
			"/uploads/eaf10b3b-3b3a-4dcd-92a7-edf2a512276e/67b8b9bf-7cca-4cb6-9b34-22fcb4d6e27d/Bildschirmfoto%202022-09-19%20um%2021.38.37.png",
		},
		{
			"double prefix",
			"//uploads/t.png",
			"/uploads/t.png",
		},
		{
			"triple prefix",
			"///uploads/t.png",
			"/uploads/t.png",
		},
		{
			"empty prefix",
			"uploads/t.png",
			"/uploads/t.png",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, urlEscapeObject(tt.args), "clean %v", tt.args)
		})
	}
}

func TestGetObjectHandlerStaticWebsiteDisabled(t *testing.T) {
	filerCalled := false
	s3a := newStaticWebsiteTestServer(t, false, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		filerCalled = true
	}))

	recorder := httptest.NewRecorder()
	s3a.GetObjectHandler(recorder, newObjectRequest(t, http.MethodGet, "/bucket/docs/", "/docs/"))

	assert.Equal(t, http.StatusNotImplemented, recorder.Code)
	assert.Contains(t, recorder.Body.String(), "<Code>NotImplemented</Code>")
	assert.False(t, filerCalled)
}

func TestGetObjectHandlerStaticWebsiteDisabledDoesNotRequestDirectoryDetection(t *testing.T) {
	s3a := newStaticWebsiteTestServer(t, false, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Empty(t, r.Header.Get(s3_constants.SeaweedFSDetectDirectory))
		_, _ = io.WriteString(w, "content")
	}))

	request := newObjectRequest(t, http.MethodGet, "/bucket/object", "/object")
	request.Header.Set(s3_constants.SeaweedFSDetectDirectory, "true")
	recorder := httptest.NewRecorder()
	s3a.GetObjectHandler(recorder, request)

	assert.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, "content", recorder.Body.String())
}

func TestGetObjectHandlerStaticWebsiteIndexDocument(t *testing.T) {
	s3a := newStaticWebsiteTestServer(t, true, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/buckets/bucket/docs/index.html", r.URL.Path)
		w.Header().Set("Content-Type", "text/html")
		_, _ = io.WriteString(w, "<h1>docs</h1>")
	}))

	recorder := httptest.NewRecorder()
	s3a.GetObjectHandler(recorder, newObjectRequest(t, http.MethodGet, "/bucket/docs/", "/docs/"))

	assert.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, "text/html", recorder.Header().Get("Content-Type"))
	assert.Equal(t, "<h1>docs</h1>", recorder.Body.String())
}

func TestGetObjectHandlerStaticWebsiteMissingIndexDocument(t *testing.T) {
	s3a := newStaticWebsiteTestServer(t, true, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/buckets/bucket/docs/index.html", r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
	}))

	recorder := httptest.NewRecorder()
	s3a.GetObjectHandler(recorder, newObjectRequest(t, http.MethodGet, "/bucket/docs/", "/docs/"))

	assert.Equal(t, http.StatusNotFound, recorder.Code)
	assert.Contains(t, recorder.Body.String(), "<Code>NoSuchKey</Code>")
	assert.Contains(t, recorder.Body.String(), "<Resource>/bucket/docs/</Resource>")
}

func TestGetObjectHandlerStaticWebsiteRegularObject(t *testing.T) {
	s3a := newStaticWebsiteTestServer(t, true, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/buckets/bucket/app.js", r.URL.Path)
		_, _ = io.WriteString(w, "main()")
	}))

	recorder := httptest.NewRecorder()
	s3a.GetObjectHandler(recorder, newObjectRequest(t, http.MethodGet, "/bucket/app.js", "/app.js"))

	assert.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, "main()", recorder.Body.String())
}

func TestGetObjectHandlerStaticWebsiteServesDirectoryWithoutTrailingSlash(t *testing.T) {
	requestCount := 0
	s3a := newStaticWebsiteTestServer(t, true, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		switch requestCount {
		case 1:
			assert.Equal(t, "/buckets/bucket/docs", r.URL.Path)
			assert.Equal(t, "true", r.Header.Get(s3_constants.SeaweedFSDetectDirectory))
			w.Header().Set(s3_constants.SeaweedFSIsDirectoryKey, "true")
		case 2:
			assert.Equal(t, "/buckets/bucket/docs/index.html", r.URL.Path)
			assert.Empty(t, r.Header.Get(s3_constants.SeaweedFSDetectDirectory))
			w.Header().Set("Content-Type", "text/html")
			_, _ = io.WriteString(w, "<h1>docs</h1>")
		default:
			t.Fatalf("unexpected filer request %d", requestCount)
		}
	}))

	recorder := httptest.NewRecorder()
	s3a.GetObjectHandler(recorder, newObjectRequest(t, http.MethodGet, "/bucket/docs?download=1", "/docs"))

	assert.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, "text/html", recorder.Header().Get("Content-Type"))
	assert.Equal(t, "<h1>docs</h1>", recorder.Body.String())
	assert.Empty(t, recorder.Header().Get("Location"))
	assert.Equal(t, 2, requestCount)
}

func TestGetObjectHandlerStaticWebsiteServesDirectoryWhenListingDisabled(t *testing.T) {
	requestCount := 0
	s3a := newStaticWebsiteTestServer(t, true, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		switch requestCount {
		case 1:
			assert.Equal(t, "/buckets/bucket/docs", r.URL.Path)
			assert.Equal(t, "true", r.Header.Get(s3_constants.SeaweedFSDetectDirectory))
			w.Header().Set(s3_constants.SeaweedFSIsDirectoryKey, "true")
			w.WriteHeader(http.StatusForbidden)
		case 2:
			assert.Equal(t, "/buckets/bucket/docs/index.html", r.URL.Path)
			assert.Empty(t, r.Header.Get(s3_constants.SeaweedFSDetectDirectory))
			_, _ = io.WriteString(w, "index")
		default:
			t.Fatalf("unexpected filer request %d", requestCount)
		}
	}))

	recorder := httptest.NewRecorder()
	s3a.GetObjectHandler(recorder, newObjectRequest(t, http.MethodGet, "/bucket/docs", "/docs"))

	assert.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, "index", recorder.Body.String())
	assert.Empty(t, recorder.Header().Get("Location"))
	assert.Equal(t, 2, requestCount)
}

func TestGetObjectHandlerStaticWebsiteMissingIndexWithoutTrailingSlash(t *testing.T) {
	requestCount := 0
	s3a := newStaticWebsiteTestServer(t, true, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		if requestCount == 1 {
			assert.Equal(t, "/buckets/bucket/docs", r.URL.Path)
			w.Header().Set(s3_constants.SeaweedFSIsDirectoryKey, "true")
			return
		}
		assert.Equal(t, "/buckets/bucket/docs/index.html", r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
	}))

	recorder := httptest.NewRecorder()
	s3a.GetObjectHandler(recorder, newObjectRequest(t, http.MethodGet, "/bucket/docs", "/docs"))

	assert.Equal(t, http.StatusNotFound, recorder.Code)
	assert.Contains(t, recorder.Body.String(), "<Code>NoSuchKey</Code>")
	assert.Contains(t, recorder.Body.String(), "<Resource>/bucket/docs</Resource>")
	assert.Equal(t, 2, requestCount)
}

func TestHeadObjectHandlerStaticWebsiteDisabled(t *testing.T) {
	s3a := newStaticWebsiteTestServer(t, false, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodHead, r.Method)
		assert.Equal(t, "/buckets/bucket/docs/", r.URL.Path)
		w.Header().Set(s3_constants.SeaweedFSIsDirectoryKey, "true")
		w.Header().Set("ETag", `"directory"`)
	}))

	recorder := httptest.NewRecorder()
	s3a.HeadObjectHandler(recorder, newObjectRequest(t, http.MethodHead, "/bucket/docs/", "/docs/"))

	assert.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, `"directory"`, recorder.Header().Get("ETag"))
}

func TestHeadObjectHandlerStaticWebsiteIndexDocument(t *testing.T) {
	s3a := newStaticWebsiteTestServer(t, true, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodHead, r.Method)
		assert.Equal(t, "/buckets/bucket/docs/index.html", r.URL.Path)
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("ETag", `"index"`)
		_, _ = io.WriteString(w, "<h1>docs</h1>")
	}))

	recorder := httptest.NewRecorder()
	s3a.HeadObjectHandler(recorder, newObjectRequest(t, http.MethodHead, "/bucket/docs/", "/docs/"))

	assert.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, "text/html", recorder.Header().Get("Content-Type"))
	assert.Equal(t, `"index"`, recorder.Header().Get("ETag"))
	assert.Empty(t, recorder.Body.String())
}

func TestHeadObjectHandlerStaticWebsiteMissingIndexDocument(t *testing.T) {
	s3a := newStaticWebsiteTestServer(t, true, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodHead, r.Method)
		assert.Equal(t, "/buckets/bucket/docs/index.html", r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
	}))

	recorder := httptest.NewRecorder()
	s3a.HeadObjectHandler(recorder, newObjectRequest(t, http.MethodHead, "/bucket/docs/", "/docs/"))

	assert.Equal(t, http.StatusNotFound, recorder.Code)
	assert.Contains(t, recorder.Body.String(), "<Code>NoSuchKey</Code>")
}

func TestHeadObjectHandlerStaticWebsiteServesDirectoryWithoutTrailingSlash(t *testing.T) {
	requestCount := 0
	s3a := newStaticWebsiteTestServer(t, true, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		assert.Equal(t, http.MethodHead, r.Method)
		switch requestCount {
		case 1:
			assert.Equal(t, "/buckets/bucket/docs", r.URL.Path)
			assert.Equal(t, "true", r.Header.Get(s3_constants.SeaweedFSDetectDirectory))
			w.Header().Set(s3_constants.SeaweedFSIsDirectoryKey, "true")
		case 2:
			assert.Equal(t, "/buckets/bucket/docs/index.html", r.URL.Path)
			assert.Empty(t, r.Header.Get(s3_constants.SeaweedFSDetectDirectory))
			w.Header().Set("Content-Type", "text/html")
			w.Header().Set("Content-Length", "13")
			w.Header().Set("ETag", `"index"`)
		default:
			t.Fatalf("unexpected filer request %d", requestCount)
		}
	}))

	recorder := httptest.NewRecorder()
	s3a.HeadObjectHandler(recorder, newObjectRequest(t, http.MethodHead, "/bucket/docs?download=1", "/docs"))

	assert.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, "text/html", recorder.Header().Get("Content-Type"))
	assert.Equal(t, `"index"`, recorder.Header().Get("ETag"))
	assert.Empty(t, recorder.Header().Get("Location"))
	assert.Empty(t, recorder.Body.String())
	assert.Equal(t, 2, requestCount)
}

func TestHeadObjectHandlerStaticWebsiteMissingIndexWithoutTrailingSlash(t *testing.T) {
	requestCount := 0
	s3a := newStaticWebsiteTestServer(t, true, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		assert.Equal(t, http.MethodHead, r.Method)
		if requestCount == 1 {
			assert.Equal(t, "/buckets/bucket/docs", r.URL.Path)
			w.Header().Set(s3_constants.SeaweedFSIsDirectoryKey, "true")
			return
		}
		assert.Equal(t, "/buckets/bucket/docs/index.html", r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
	}))

	recorder := httptest.NewRecorder()
	s3a.HeadObjectHandler(recorder, newObjectRequest(t, http.MethodHead, "/bucket/docs", "/docs"))

	assert.Equal(t, http.StatusNotFound, recorder.Code)
	assert.Contains(t, recorder.Body.String(), "<Code>NoSuchKey</Code>")
	assert.Equal(t, 2, requestCount)
}

func newStaticWebsiteTestServer(t *testing.T, enabled bool, filerHandler http.Handler) *S3ApiServer {
	t.Helper()
	filerServer := httptest.NewServer(filerHandler)
	t.Cleanup(filerServer.Close)

	return &S3ApiServer{
		option: &S3ApiServerOption{
			Filer:               pb.ServerAddress(strings.TrimPrefix(filerServer.URL, "http://")),
			BucketsPath:         "/buckets",
			EnableStaticWebsite: enabled,
		},
		client:     filerServer.Client(),
		filerGuard: security.NewGuard(nil, "", 0, "", 0, "", ""),
	}
}

func newObjectRequest(t *testing.T, method, target, object string) *http.Request {
	t.Helper()
	r := httptest.NewRequest(method, target, nil)
	return mux.SetURLVars(r, map[string]string{
		"bucket": "bucket",
		"object": object,
	})
}
