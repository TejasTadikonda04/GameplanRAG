#!/usr/bin/env Rscript
# ---------------------------------------------------------------------------
# fetch_nflfastR_data.R
# ---------------------------------------------------------------------------
# Download every regular-season play-by-play file (1999-present) and store
# them in data/lake/pbp/ as compressed Parquet files, one per season.
#
#   Rscript src/scripts/fetch_nflfastR_data.R
# ---------------------------------------------------------------------------

# ── 1. Packages ────────────────────────────────────────────────────────────
required <- c("nflfastR", "arrow", "dplyr", "cli")
missing  <- setdiff(required, rownames(installed.packages()))
if (length(missing)) install.packages(
  missing,
  repos = c(
    nflverse = "https://nflverse.r-universe.dev",   # where nflfastR lives
    CRAN     = "https://cloud.r-project.org"
  ),
  type = "binary"    # use Windows/Mac binaries when available
)
lapply(required, library, character.only = TRUE)

# ── 2. Parameters ──────────────────────────────────────────────────────────
first_season <- 1999
last_season  <- as.integer(format(Sys.Date(), "%Y")) - 1   # complete through last yr
seasons      <- first_season:last_season
out_dir      <- "data/lake/pbp"                            # *single* target folder
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

# ── 3. Download loop ───────────────────────────────────────────────────────
download_season <- function(yr) {
  cli::cli_h2("Season {yr}")
  df <- nflfastR::load_pbp(yr) |>
        dplyr::filter(season_type == "REG")

  file <- file.path(out_dir, sprintf("pbp_%d.parquet", yr))
  arrow::write_parquet(
    df,
    file,
    compression = "zstd",
    compression_level = 12
  )
  cli::cli_alert_success("✔ {nrow(df)} rows → {.file {file}}")
}

purrr::walk(seasons, download_season)

cli::cli_alert_success(
  "🎉 All seasons {first_season}:{last_season} saved in {normalizePath(out_dir)}"
)
