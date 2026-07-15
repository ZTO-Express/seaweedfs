## 1. Configuration

- [x] 1.1 Add the disabled-by-default static website option to `S3Options` and `S3ApiServerOption`.
- [x] 1.2 Register the standalone and embedded command flags and pass the value into the S3 API server.

## 2. Request Handling

- [x] 2.1 Resolve trailing-slash `GET` requests to the directory's `index.html` when the option is enabled.
- [x] 2.2 Redirect a filer-confirmed directory request without a trailing slash to the trailing-slash URL.
- [x] 2.3 Preserve disabled-mode and normal object behavior, including standard `NoSuchKey` errors for a missing index document.
- [x] 2.4 Apply the static website index and redirect semantics consistently to `HEAD` requests while preserving disabled-mode HeadObject behavior.

## 3. Verification

- [x] 3.1 Add focused handler tests for enabled, disabled, existing index, missing index, normal object, and redirect behavior.
- [x] 3.2 Run Go formatting and the focused S3 API and command package tests.
- [x] 3.3 Add focused `HEAD` tests for enabled index resolution, missing indexes, redirects, and disabled-mode behavior.
