## Context

`GetObjectHandler` currently rejects paths ending in `/`. For paths without a trailing slash, the filer can identify directory marker objects through `X-Seaweedfs-Is-Directory-Key`. Static website hosting needs to translate those directory accesses into an object read while retaining the existing IAM and filer proxy path.

## Goals / Non-Goals

- Goals: provide an opt-in, gateway-wide `index.html` resolution mode for directory `GET` and `HEAD` requests; preserve normal object reads and disabled-mode behavior; keep all three S3 startup modes consistent.
- Non-Goals: implement the AWS bucket website configuration APIs, custom index filenames, custom error documents, directory listings, or per-bucket website settings.

## Decisions

- Decision: add a boolean `EnableStaticWebsite` server option and matching command flags. The default is `false` to avoid changing S3 API semantics for existing installations.
- Decision: use the fixed index document name `index.html`, matching the requested scope and avoiding configuration combinations that imply broader AWS website API compatibility.
- Decision: a `GET` or `HEAD` request already ending in `/` is internally proxied to `<directory>/index.html`; the client URL does not change.
- Decision: a `GET` or `HEAD` request without `/` that the filer identifies as a directory returns a permanent redirect to the same URL with `/`. Browsers then resolve relative CSS, JavaScript, image, and link URLs from the correct directory base.
- Decision: reuse the existing filer proxy behavior for the index request so range requests, conditional headers, response overrides, filer JWT authorization, errors, logging, and response streaming remain consistent.
- Decision: static website mode has website semantics for both `GET` and `HEAD`. `HEAD` resolves and redirects exactly like `GET`, returning the selected index object's status and headers without a response body. When the mode is disabled, `HeadObject` retains exact-key S3 semantics.

## Risks / Trade-offs

- A gateway-wide option affects every bucket served by that gateway. This is intentionally smaller than implementing per-bucket website configuration; deployments requiring isolation can use a dedicated gateway instance.
- Internally changing the filer target means the S3 error resource remains the originally requested directory URL. This is desirable for clients and avoids leaking the internal index lookup path.
- Redirecting directory URLs without `/` adds one browser round trip, but prevents broken relative asset paths.

## Migration Plan

No migration is required. Existing deployments remain unchanged. Operators enable the new command option and restart the S3 gateway.
