# Pulp-ext-s3

S3 storage capability for Pulp cells, backed by AWS SDK v2. Compatible with Cloudflare R2 via path-style addressing.

From [BananaLabs OSS](https://github.com/BananaLabs-OSS).

## Deployment

```go
import _ "github.com/BananaLabs-OSS/Pulp-ext-s3"
```

## Capability

- `storage.s3` — get, put (plain + sized), multipart upload (init / part / complete / abort), head, copy, delete, list, presign (GET + PUT)

## Environment

- `S3_ACCOUNT_ID` — Cloudflare R2 account ID (used to build endpoint)
- `S3_ACCESS_KEY_ID` — access key
- `S3_SECRET_ACCESS_KEY` — secret key
- `S3_BUCKET` — bucket name cells operate against
- `S3_ENDPOINT` — optional; overrides the R2 endpoint for local dev (minio, localstack) or other S3-compatible backends
