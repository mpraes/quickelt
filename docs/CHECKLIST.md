# Project Best Practices Checklist

Use this checklist to keep quality, maintainability, and operational safety high as the project evolves.

## Structure and Organization

- [x] Keep raw data isolated under `data/bronze/`.
- [x] Keep ingestion metadata in a dedicated `metadata/` path.
- [x] Organize ingestion templates by framework and source type.
- [x] Keep automated tests under `infrastructure/test/` and related test paths.
- [x] Keep reusable shared logic in utility modules.

## Development Practices

- [x] Use deterministic file naming conventions with source and timestamp metadata.
- [x] Generate metadata for ingestion runs where applicable.
- [x] Load configuration from environment variables, not hardcoded secrets.
- [x] Ensure required directories exist before writing outputs.
- [x] Centralize defaults/constants to avoid value drift.

## Testing Practices

- [x] Maintain automated tests for setup orchestration and provisioner behavior.
- [x] Cover failure paths and cancellation flows, not only happy paths.
- [x] Validate Terraform module formatting and schema correctness.
- [x] Validate workflow configuration (`.github/CI.yml`) through linting and test assertions.

## Security Practices

- [x] Do not commit real credential files.
- [x] Restrict permission for generated credential-bearing files (`0600`).
- [x] Prefer least-privilege cloud identities.
- [x] Keep local development credentials separate from production credentials.

## Planned Improvements

- [ ] Expand runtime integration tests for Terraform destroy and cleanup edge cases.
- [ ] Add end-to-end smoke tests for CLI setup in CI-compatible environments.
- [ ] Add docs quality checks (link checks and markdown linting).
