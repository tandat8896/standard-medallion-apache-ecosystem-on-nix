# Atlas Schema Migration Configuration
env "local" {
  src = "file://ETL/infrastructure/postgres-schemas/create_gold_tables.sql"
  url = "postgres://localhost:5432/etl_analytics?sslmode=disable"

  migration {
    dir = "file://migrations"
  }
}

env "staging" {
  url = env("ATLAS_STAGING_URL")
  migration {
    dir = "file://migrations"
  }
}

env "prod" {
  url = env("ATLAS_PROD_URL")
  migration {
    dir = "file://migrations"
  }

  lint {
    destructive {
      error = true
    }
  }
}
