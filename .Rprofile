## .Rprofile  (runs at the start of every R/Rscript session in this folder)
local({
  proj_lib <- file.path("data", "Rlibs")
  if (!dir.exists(proj_lib)) dir.create(proj_lib, recursive = TRUE)
  .libPaths(c(normalizePath(proj_lib, winslash = "/"), .libPaths()))
  options(repos = c(
    nflverse = "https://nflverse.r-universe.dev",
    CRAN     = "https://cloud.r-project.org"
  ))
})
