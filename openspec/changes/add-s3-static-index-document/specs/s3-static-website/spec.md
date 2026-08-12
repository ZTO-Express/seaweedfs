## ADDED Requirements

### Requirement: Opt-In Static Website Mode

The S3 gateway SHALL provide a static website hosting mode that is disabled by default and can be enabled consistently for standalone and embedded S3 gateway startup modes.

#### Scenario: Static website mode is disabled

- **WHEN** an operator starts an S3 gateway without enabling static website mode
- **THEN** directory object requests SHALL retain their existing S3 API behavior

#### Scenario: Static website mode is enabled

- **WHEN** an operator enables static website mode for an S3 gateway
- **THEN** the gateway SHALL enable index document resolution for directory `GET` and `HEAD` requests served by that gateway

### Requirement: Directory Index Document Resolution

When static website mode is enabled, the S3 gateway SHALL use `index.html` as the index document for directory `GET` and `HEAD` requests.

#### Scenario: Trailing-slash directory has an index document

- **WHEN** a client sends `GET` or `HEAD` for a directory object path ending in `/`
- **AND** `index.html` exists in that directory
- **THEN** the gateway SHALL return the index document response without redirecting the client
- **AND** a `HEAD` response SHALL contain the same status and headers as `GET` without returning the object body

#### Scenario: Trailing-slash directory has no index document

- **WHEN** a client sends `GET` or `HEAD` for a directory object path ending in `/`
- **AND** `index.html` does not exist in that directory
- **THEN** the gateway SHALL return the S3 `NoSuchKey` error for the original directory request

#### Scenario: Object is not a directory

- **WHEN** a client sends `GET` or `HEAD` for a regular object
- **THEN** the gateway SHALL return that object using the existing GetObject or HeadObject behavior for the request method

### Requirement: Directory URL Without Trailing Slash

When static website mode is enabled, the S3 gateway SHALL resolve a directory URL without a trailing slash to its index document without redirecting the client.

#### Scenario: Directory URL omits trailing slash

- **WHEN** a client sends `GET` or `HEAD` for an object path without a trailing slash
- **AND** the filer identifies that path as a directory
- **AND** `index.html` exists in that directory
- **THEN** the gateway SHALL internally request the directory's `index.html` using the original HTTP method
- **AND** the gateway SHALL return the index document response without redirecting the client

#### Scenario: Directory URL without trailing slash has no index document

- **WHEN** a client sends `GET` or `HEAD` for an object path without a trailing slash
- **AND** the filer identifies that path as a directory
- **AND** `index.html` does not exist in that directory
- **THEN** the gateway SHALL return the S3 `NoSuchKey` error for the original directory request
