# Atlas Schema Migration Configuration
# Security: ALL passwords use environment variables, NO hardcoded credentials!

env "local" {
  url = getenv("ATLAS_LOCAL_URL")
  
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
