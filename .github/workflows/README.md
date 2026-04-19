# GitHub Actions Workflows

## Workflows

### 1. build-cache.yml
Builds CI/CD nix shell and pushes to Cachix.
- Triggers: Push to main, PR on flake.nix
- Duration: ~2-3 min (first build), ~30s (cached)

### 2. schema-cicd.yml  
Validates Atlas migrations and deploys to staging.
- Triggers: PR/push on migrations/
- Duration: ~1 min (with Cachix)

### 3. test-postgres-load.yml
Tests PostgreSQL schema and data loading.
- Triggers: PR on scripts/ or schemas/
- Duration: ~2 min

### 4. schema-lint.yml (existing)
Basic migration validation.

### 5. schema-deploy-staging.yml (existing)
Staging deployment.

### 6. schema-deploy-prod.yml (existing)
Production deployment (manual trigger).

## Secrets Required

- `CACHIX_AUTH_TOKEN` - For pushing to cache
- `ATLAS_STAGING_URL` - Staging DB connection (optional)
- `ATLAS_PROD_URL` - Prod DB connection (optional)

## Local Testing

```bash
# Test build-cache workflow
nix build .#devShells.x86_64-linux.cicd

# Test schema workflow  
just schema-lint
just schema-hash

# Test postgres workflow
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:15
just create-postgres-schemas
```
