# Change: Add S3 Static Website Index Document Resolution

## Why

The S3 gateway currently returns `NotImplemented` for object paths ending in `/`, so a public bucket cannot serve a conventional static site from directory paths. Operators need an opt-in mode that resolves a directory request to the directory's `index.html` without changing normal S3 object behavior.

## What Changes

- Add an S3 static website hosting option, disabled by default.
- When enabled, serve `index.html` for a `GET` or `HEAD` request whose object path ends in `/`.
- When enabled and a `GET` or `HEAD` request without a trailing slash resolves to a directory, serve that directory's `index.html` internally without redirecting the client.
- Keep `GET` and `HEAD` website routing consistent while preserving the standard S3 object semantics when static website mode is disabled.
- Return the existing S3 `NoSuchKey` response when the resolved `index.html` does not exist.
- Expose the option consistently from the standalone S3 command and the embedded S3 gateways in `weed filer` and `weed server`.
- Preserve the current behavior when static website hosting is disabled.

## Impact

- Affected specs: `s3-static-website`
- Affected code: `weed/s3api/s3api_object_handlers.go`, `weed/s3api/s3api_server.go`, `weed/command/s3.go`, `weed/command/filer.go`, `weed/command/server.go`
- Compatibility: the option defaults to disabled, so existing S3 API behavior is unchanged unless explicitly enabled.
