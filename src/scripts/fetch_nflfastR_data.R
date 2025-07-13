#!/usr/bin/env Rscript
# ------------------------------------------------------------------
# fetch_nflfastR_data.R
#
# Loads NFL play-by-play data (1999-present) with nflreadr and
# uploads each season to its own table in Google BigQuery.
#
# Environment variables you MUST set before running:
#   BQ_PROJECT_ID  – BigQuery project ID (see Cloud console)
#
# Optional overrides (defaults shown):
#   BQ_DATASET     – Dataset name to hold the tables   ("nflfastR")
#   START_SEASON   – First season to load              (1999)
#   END_SEASON     – Last season to load               (most recent)
#
# Usage:
#   export BQ_PROJECT_ID=my-gameplan-proj
#   Rscript fetch_nflfastR_data.R
# ------------------------------------------------------------------

suppressPackageStartupMessages({
  library(nflreadr)   # fast play-by-play loader
  library(bigrquery)  # BigQuery client
  library(dplyr)
  library(purrr)
})

# ---- configuration ----
project_id <- Sys.getenv("BQ_PROJECT_ID")
if (project_id == "") stop("Set BQ_PROJECT_ID env-var first.")

dataset_id <- Sys.getenv("BQ_DATASET", unset = "nflfastR")

start_season <- as.integer(Sys.getenv("START_SEASON", unset = "1999"))
end_season   <- {
  env <- Sys.getenv("END_SEASON", unset = "")
  if (nzchar(env)) as.integer(env) else nflreadr::most_recent_season()
}

# ---- ensure dataset exists ----
ds <- bq_dataset(project_id, dataset_id)
if (!bq_dataset_exists(ds)) {
  message("Creating dataset: ", dataset_id)
  bq_dataset_create(ds)
}

# ---- season loop ----
seasons <- start_season:end_season
walk(seasons, function(yr) {
  message("\n=== Season ", yr, " ===")
  pbp <- load_pbp(yr)  # ~2-15 MB per season zipped, 50-200 MB in RAM

  tbl <- bq_table(project_id, dataset_id, paste0("pbp_", yr))
  bq_table_upload(
    tbl,
    pbp,
    write_disposition = "WRITE_TRUNCATE",   # replace if rerun
    create_disposition = "CREATE_IF_NEEDED",
    quiet = TRUE
  )
  rm(pbp); gc()
})

message("\n✅  Upload complete!  Verify tables in the BigQuery console.")
