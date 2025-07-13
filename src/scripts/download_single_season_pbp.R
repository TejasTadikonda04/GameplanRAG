#!/usr/bin/env Rscript
# ---------------------------------------------------------------------------
# download_pbp_2009.R  – one-season play-by-play lake (2009, REG only)
# ---------------------------------------------------------------------------

# ── 1. Packages ────────────────────────────────────────────────────────────
required <- c("nflfastR", "arrow", "dplyr", "cli")
new_pkgs <- setdiff(required, rownames(installed.packages()))
if (length(new_pkgs)) install.packages(
  new_pkgs,
  repos = c(
    nflverse = "https://nflverse.r-universe.dev",
    CRAN     = "https://cloud.r-project.org"
  ),
  type = "binary"       # always pulls Windows binaries when available
)
lapply(required, library, character.only = TRUE)

# ── 2. Parameters (hard-coded) ─────────────────────────────────────────────
season     <- 2009
out_root   <- "data/lake/pbp"                     # change if you prefer
out_folder <- sprintf("%s/season=%d", out_root, season)
out_file   <- sprintf("%s/pbp_%d.parquet", out_folder, season)

# ── 3. Ensure directory structure exists ───────────────────────────────────
dir.create(out_folder, recursive = TRUE, showWarnings = FALSE)

# ── 4. Download & store play-by-play ───────────────────────────────────────
cli::cli_h2("Downloading {season} regular-season play-by-play")
pbp <- nflfastR::load_pbp(season) |>
       dplyr::filter(season_type == "REG")

arrow::write_parquet(
  pbp,
  out_file,
  compression = "zstd",
  compression_level = 12
)
cli::cli_alert_success("Saved {.file {out_file}} ({nrow(pbp)} rows)")

# ── 5. Done ────────────────────────────────────────────────────────────────
cli::cli_alert_success("✅ Finished. Data lives in {normalizePath(out_folder)}")
