#!/usr/bin/env Rscript
# ---------------------------------------------------------------------------
# fetch_nflfastR_data.R
# ---------------------------------------------------------------------------
# Download every regular-season pbp file (1999-present) and upload each
# compressed Parquet to a Google Cloud Storage bucket.
#
#   Rscript src/scripts/fetch_nflfastR_data.R
# ---------------------------------------------------------------------------

# ── 1. Packages ────────────────────────────────────────────────────────────
required <- c("nflfastR", "arrow", "dplyr", "cli", "googleCloudStorageR", "purrr")
missing  <- setdiff(required, rownames(installed.packages()))
if (length(missing)) install.packages(
  missing,
  repos = c(
    nflverse = "https://nflverse.r-universe.dev",
    GCS      = "https://gcs.r-universe.dev",
    CRAN     = "https://cloud.r-project.org"
  ),
  type = "binary"
)
lapply(required, library, character.only = TRUE)

# ── 2. GCS auth & bucket ───────────────────────────────────────────────────
# Either GOOGLE_APPLICATION_CREDENTIALS *or* GCS_AUTH_FILE must be set
googleCloudStorageR::gcs_auth()                       # auto-detects env-vars
bucket <- Sys.getenv("GCS_BUCKET", "my-nfl-rag-bucket")

# ── 3. Seasons & local scratch dir ────────────────────────────────────────
first_season <- 1999
last_season  <- as.integer(format(Sys.Date(), "%Y")) - 1
seasons      <- first_season:last_season
scratch_dir  <- "data/lake/pbp"
dir.create(scratch_dir, recursive = TRUE, showWarnings = FALSE)

# ── 4. Download ➜ Parquet ➜ Upload ────────────────────────────────────────
push_season <- function(yr) {
  cli::cli_h2("Season {yr}")
  df <- nflfastR::load_pbp(yr) |> dplyr::filter(season_type == "REG")

  fname_local <- file.path(scratch_dir, sprintf("pbp_%d.parquet", yr))
  arrow::write_parquet(df, fname_local,
                       compression = "zstd", compression_level = 12)

  # Upload to gs://<bucket>/pbp/<file>
  gcs_path <- file.path("pbp", basename(fname_local))
  googleCloudStorageR::gcs_upload(
    file          = fname_local,
    bucket        = bucket,
    name          = gcs_path,
    predefinedAcl = "bucketLevel"   # obey bucket-level IAM
  )

  cli::cli_alert_success("✔ {nrow(df)} rows → gs://{bucket}/{gcs_path}")
}

purrr::walk(seasons, push_season)
cli::cli_alert_success(
  "🎉 All seasons {first_season}:{last_season} uploaded to bucket {bucket}"
)
