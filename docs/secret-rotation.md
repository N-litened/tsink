# Secret Rotation

tsink supports runtime rotation of all security-sensitive materials without restarting the server. This covers bearer tokens (public, admin, and cluster-internal), TLS certificates and keys for the HTTP listener, mTLS materials for cluster peer-to-peer traffic, and service account tokens stored in the RBAC configuration.

---

## Overview

Every managed secret has two orthogonal operations:

| Operation | Meaning |
|-----------|---------|
| **Reload** | Re-read the credential from its backing source (file or exec command). The credential value may or may not change depending on what the source returns. |
| **Rotate** | Generate or apply a new credential value, persist it to the backing source, and optionally accept the previous credential for a configurable overlap window. |

Bearer tokens additionally support an **overlap grace period**: immediately after rotation the old token is still accepted for a configurable number of seconds (default 300 s), allowing in-flight clients and automation to catch up before the old credential is rejected.

---

## Credential sources

### File-backed (plain)

A plain text file contains the raw credential value. This source supports reload (re-read the file on demand). Rotating a file-backed public or admin token makes tsink generate a new value (or use `newValue`) and write it to the file. For other file-backed secrets tsink does not generate a value, so writing a new value into the file from outside tsink, then calling `reload`, is the typical workflow.

### Exec-backed (manifest)

A JSON manifest file describes external commands for loading and, optionally, rotating the credential. If the file's contents start with `{`, tsink parses it as a manifest rather than reading it as a raw value.

```json
{
  "kind": "exec",
  "provider": "vault",
  "command": ["vault", "kv", "get", "-field=value", "secret/tsink/token"],
  "rotateCommand": ["vault", "kv", "put", "secret/tsink/token", "value=$NEW_TOKEN"]
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `kind` | yes | Must be `"exec"`. |
| `provider` | no | Human-readable label shown in error messages (e.g. `"vault"`, `"aws-secrets"`). |
| `command` | yes | Command to run to load the current credential. stdout is the credential value; non-zero exit is an error. |
| `rotateCommand` | no | Command to run when rotation is requested. Required for `rotate` operations against exec-backed secrets. |

### Inline

Inline tokens are supplied directly via a CLI flag (`--auth-token <TOKEN>`). They are **not reloadable or rotatable** at runtime — a server restart is necessary to change an inline token.

---

## Bearer token rotation

### Configuration

| CLI flag | Description |
|----------|-------------|
| `--auth-token <TOKEN>` | Public API bearer token (inline; restart-only). |
| `--auth-token-file <PATH>` | Path to a plain file or exec manifest for the public API token. |
| `--admin-auth-token <TOKEN>` | Admin API bearer token (inline; restart-only). |
| `--admin-auth-token-file <PATH>` | Path to a plain file or exec manifest for the admin token. |
| `--cluster-internal-auth-token <TOKEN>` | Cluster peer RPC token (inline; restart-only). |
| `--cluster-internal-auth-token-file <PATH>` | Path to a plain file or exec manifest for the internal cluster token. |

### Rotation targets

| `target` value | Token protected |
|----------------|----------------|
| `publicAuthToken` | Public API endpoints (`Authorization: Bearer …`). |
| `adminAuthToken` | Admin API endpoints (`/api/v1/admin/…`). |
| `clusterInternalAuthToken` | Cluster peer RPC calls. |

### Overlap grace period

When a bearer token is rotated the previous credential is retained in memory with an expiry timestamp equal to `now + overlapSeconds`. Any request that presents the previous credential is accepted until that expiry, after which only the new credential is valid.

- **Default:** 300 seconds (5 minutes).
- **Disable immediately:** set `overlapSeconds` to `0`.
- The expiry is checked on every authentication attempt; no background task is needed.

### Reload vs. rotate for file-backed tokens

**Reload** — tsink re-reads the file. If an external system has already written a new token to the file (e.g. a secrets manager sync), this makes it active.

**Rotate** — tsink either:
- Generates a new cryptographically random 32-byte token (base64url-encoded) and writes it to the backing file, or
- Calls `rotateCommand` on exec-backed secrets and then reloads the result.

An explicit `newValue` can be supplied in the request to set an exact token value instead of generating one. It must not be empty and must not start with `{` (a token file starting with `{` would be read as an exec manifest). The cluster internal token is never generated: rotating it requires `newValue` or an exec manifest with `rotateCommand`.

---

## TLS certificate rotation

### Configuration

| CLI flag | Description |
|----------|-------------|
| `--tls-cert <PATH>` | Server certificate chain (PEM). Plain file or exec manifest. |
| `--tls-key <PATH>` | Server private key (PEM). Plain file or exec manifest. |

### How hot-reload works

tsink uses rustls. When a reload or rotate is triggered, a new `ServerConfig` and `TlsAcceptor` are built in-place from the updated PEM material. Existing connections are unaffected; new connections negotiate TLS with the updated certificate.

### Rotation target

| `target` value | Material rotated |
|----------------|-----------------|
| `listenerTls` | HTTP listener certificate + key. |

**Reload** re-reads the PEM files from disk without executing any external command. Use this when your certificate authority has already dropped a renewed certificate at the configured path.

**Rotate** executes the `rotateCommand` hook in the exec manifest, then reloads the resulting material. File-backed (non-exec) TLS does not support rotation; update the file externally and use reload instead.

---

## Cluster mTLS rotation

### Configuration

| CLI flag | Description |
|----------|-------------|
| `--cluster-internal-mtls-enabled <bool>` | Enable mTLS for intra-cluster RPC (`--cluster-internal-mtls-enabled true`). |
| `--cluster-internal-mtls-ca-cert <PATH>` | CA bundle used to verify peer certificates. |
| `--cluster-internal-mtls-cert <PATH>` | Client certificate presented on outbound RPC. |
| `--cluster-internal-mtls-key <PATH>` | Client private key for outbound RPC. |

When mTLS is enabled all three paths (CA cert, cert, key) are required. Providing only some of them is a configuration error.

### Rotation target

| `target` value | Material rotated |
|----------------|-----------------|
| `clusterInternalMtls` | CA bundle, client cert, and client key for peer RPC. |

RPC clients reload the mTLS material per-request from the managed bundle, so a rotation takes effect on the next outbound call with no connection-level disruption.

---

## Service account token rotation

Service accounts are defined in the RBAC configuration file (see [Security model](security.md)). Each account carries a single `token` value. Rotation replaces that token with a new cryptographically random 32-byte value (base64url-encoded), updates the `lastRotatedUnixMs` timestamp, and persists the change back to the RBAC file.

Unlike bearer tokens, service account rotation has **no overlap window** — the new token takes effect immediately. Plan rotation windows accordingly.

The new token value is returned in the API response exactly once; it is not stored in plaintext anywhere else.

---

## HTTP API

All rotation and status endpoints require `--enable-admin-api` and the admin credential.

### Rotate a secret

```
POST /api/v1/admin/secrets/rotate
```

**Request body (JSON):**

```json
{
  "target": "publicAuthToken",
  "mode": "rotate",
  "newValue": null,
  "overlapSeconds": 300
}
```

Field names are camelCase and unknown fields are rejected.

| Field | Type | Description |
|-------|------|-------------|
| `target` | string | Required. One of: `publicAuthToken`, `adminAuthToken`, `clusterInternalAuthToken`, `listenerTls`, `clusterInternalMtls`. |
| `mode` | string | `"rotate"` (default) or `"reload"`. |
| `newValue` | string \| null | Explicit new token value; omit or set to `null` to generate one. Must not be empty or start with `{`. Rejected with `"reload"` and for exec-backed secrets. Ignored for TLS targets. |
| `overlapSeconds` | integer \| null | Grace period in seconds for the previous bearer token. Omitted or `null` uses the default (300 s). `0` disables overlap. |

**Response:**

```json
{
  "status": "success",
  "data": {
    "target": "publicAuthToken",
    "mode": "rotate",
    "issuedCredential": "<new-token>",
    "state": { "kind": "token", "target": "publicAuthToken", "generation": 4, ... }
  }
}
```

`issuedCredential` is the token value the server wrote to the backing file; it is `null` for `reload`, TLS targets, and exec-backed secrets. `state` is the target's snapshot (see below). Errors return `400` with a plain-text message.

### Rotate a service account token

```
POST /api/v1/admin/rbac/service_accounts/rotate
```

**Request body (JSON):**

```json
{
  "id": "my-service-account"
}
```

**Response:**

```json
{
  "status": "success",
  "data": {
    "serviceAccount": {
      "id": "my-service-account",
      "description": "CI pipeline account",
      "disabled": false,
      "createdUnixMs": 1700000000000,
      "updatedUnixMs": 1700001000000,
      "lastRotatedUnixMs": 1700001000000,
      "bindings": [...]
    },
    "token": "<new-token>"
  }
}
```

The `data.token` field contains the new credential. Store it securely — it will not be returned again.

### Inspect current state

```
GET /api/v1/admin/secrets/state
```

Returns the state of every configured secret in `data.targets` (an array, one snapshot per target) and the rolling audit log of the last 128 reload and rotate operations in `data.auditEntries`.

**Example response (abbreviated):**

```json
{
  "status": "success",
  "data": {
    "enabled": true,
    "targets": [
      {
        "kind": "token",
        "target": "publicAuthToken",
        "restartSafe": true,
        "reloadable": true,
        "rotatable": true,
        "generation": 3,
        "lastLoadedUnixMs": 1700001200000,
        "lastRotatedUnixMs": 1700001200000,
        "previousCredentialExpiresUnixMs": 1700001500000,
        "acceptsPreviousCredential": true,
        "source": { "path": "/run/secrets/tsink-token", "sourceKind": "file", "rotateSupported": false }
      },
      {
        "kind": "tls",
        "target": "listenerTls",
        "restartSafe": true,
        "reloadable": true,
        "rotatable": false,
        "generation": 1,
        "lastLoadedUnixMs": 1700000000000,
        "lastRotatedUnixMs": 0,
        "cert": { "path": "/etc/tsink/server.pem", "sourceKind": "file", "rotateSupported": false },
        "key": { "path": "/etc/tsink/server.key", "sourceKind": "file", "rotateSupported": false }
      }
    ],
    "auditEntries": [
      {
        "sequence": 7,
        "timestampUnixMs": 1700001200000,
        "target": "publicAuthToken",
        "operation": "rotate",
        "outcome": "success",
        "actor": "admin"
      }
    ],
    "serviceAccounts": null
  }
}
```

**Snapshot fields for bearer tokens:**

| Field | Description |
|-------|-------------|
| `kind` | `"token"` for bearer tokens, `"tls"` for TLS targets. |
| `target` | The rotation target this snapshot describes. |
| `restartSafe` | `true` if the token comes from a file or exec manifest, so a rotated value survives a restart; `false` for inline tokens. |
| `reloadable` | `true` if the token can be reloaded without a restart. |
| `rotatable` | `true` if in-place rotation is supported. |
| `generation` | Incremented each time a reload or rotate changes the value. Clients can poll this to detect changes. |
| `lastLoadedUnixMs` | Unix timestamp (ms) of the most recent successful load. |
| `lastRotatedUnixMs` | Unix timestamp (ms) of the most recent rotation; `0` if never rotated. |
| `acceptsPreviousCredential` | `true` during the overlap window after rotation. |
| `previousCredentialExpiresUnixMs` | When the previous credential expires; omitted if no overlap is active. |

---

## Audit log

Every reload and rotate operation — successful or not — is appended to an in-memory audit buffer, returned as `data.auditEntries` by `GET /api/v1/admin/secrets/state`.

| Field | Description |
|-------|-------------|
| `sequence` | Monotonically increasing counter. |
| `timestampUnixMs` | When the operation occurred. |
| `target` | Which secret was affected. |
| `operation` | `"reload"` or `"rotate"`. |
| `outcome` | `"success"` or `"failure"`. |
| `actor` | ID of the principal that triggered the operation, if authenticated. |
| `detail` | Error message on failure; omitted on success. |

The security audit buffer holds up to **128 entries** (FIFO eviction). Service account operations are recorded in a separate RBAC audit buffer with a capacity of **256 entries**, accessible via `GET /api/v1/admin/rbac/audit`.

---

## Common error conditions

| Error | Meaning |
|-------|---------|
| `"is not configured"` | The target secret was not provided at startup (e.g. no `--auth-token-file` flag). |
| `"cannot be rotated without a restart"` | The secret was supplied inline; restart with a file-backed source to enable runtime rotation. |
| `"is configured inline and cannot be reloaded"` | Same as above, for reload. |
| `"does not have any rotateCommand hooks configured"` | TLS rotation requires an exec-backed manifest with a `rotateCommand`. |
| `"does not accept newValue writes"` | Exec-backed secrets manage their own values; omit `newValue` or set it to `null`. |
| `"reload does not accept newValue"` | `newValue` is only valid with `"mode": "rotate"`. |
| `"rotation requires an explicit newValue"` | The target cannot generate a token automatically (the cluster internal token); include `newValue` in the request. |
| `"newValue must not be empty"` / `"newValue must not start with '{'"` | The supplied `newValue` was rejected. |

---

## Operational recipes

### Zero-downtime bearer token rotation

```bash
# 1. Trigger rotation. The new token is returned; the old one is still accepted
#    for 300 seconds (default overlap).
curl -s -X POST http://localhost:9201/api/v1/admin/secrets/rotate \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"target":"publicAuthToken","mode":"rotate","overlapSeconds":300}' \
  | jq -r '.data.issuedCredential'      # distribute this value to clients

# 2. Update all clients within the 300-second window.

# 3. Confirm the old credential is no longer accepted (overlap expired or use
#    overlapSeconds:0 on the next rotation to skip the window entirely).
```

### Reload a TLS certificate renewed by an external CA

```bash
# The CA has already written the new cert to the configured path.
curl -X POST http://localhost:9201/api/v1/admin/secrets/rotate \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"target":"listenerTls","mode":"reload"}'
```

### Rotate a service account token via the API

```bash
NEW_TOKEN=$(curl -s -X POST \
  http://localhost:9201/api/v1/admin/rbac/service_accounts/rotate \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"id":"ci-pipeline"}' | jq -r '.data.token')

# Store $NEW_TOKEN in your secrets manager before it is lost.
```

### Check rotation status and overlap expiry

```bash
curl -s http://localhost:9201/api/v1/admin/secrets/state \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  | jq '.data.targets[] | select(.target == "publicAuthToken") | {generation, acceptsPreviousCredential, previousCredentialExpiresUnixMs}'
```
