# Atlas Schema Migration Configuration
# Security: ALL passwords use environment variables, NO hardcoded credentials!

env "local" {
  src = "file://ETL/infrastructure/postgres-schemas/create_gold_tables.sql"
  # Use ATLAS_LOCAL_URL env var, fallback to default if not set
  url = getenv("ATLAS_LOCAL_URL") != "" ? getenv("ATLAS_LOCAL_URL") : "postgres://tandat8896-nix@localhost:5432/etl_analytics?sslmode=disable"

  migration {
    dir = "file://migrations"
  }
}

env "staging" {
  # Must set ATLAS_STAGING_URL environment variable
  url = getenv("ATLAS_STAGING_URL")
  migration {
    dir = "file://migrations"
  }
}

env "prod" {
  # Must set ATLAS_PROD_URL environment variable
  url = getenv("ATLAS_PROD_URL")
  migration {
    dir = "file://migrations"
  }

  lint {
    destructive {
      error = true
    }
  }
}
