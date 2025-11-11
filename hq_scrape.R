pkgs <- c("httr","jsonlite","dplyr","tidyr","stringr","lubridate","arrow")
to_install <- setdiff(pkgs, rownames(installed.packages()))
if (length(to_install)) install.packages(to_install, repos="https://cloud.r-project.org")
suppressPackageStartupMessages({library(dplyr); library(lubridate); library(arrow)})

TZ <- "America/Toronto"
source("hq_functions.R")

df <- get_hq_outages(FALSE) |> mutate(retrieved_at = with_tz(Sys.time(), TZ))
if (!nrow(df)) df <- tibble::as_tibble(df) |> mutate(retrieved_at = with_tz(Sys.time(), TZ))

ts <- with_tz(Sys.time(), TZ)
out_dir <- file.path("data",
                     paste0("date=", format(ts, "%Y-%m-%d")),
                     paste0("hour=", format(ts, "%H")))
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

pq_path <- file.path(out_dir, paste0("outages_", format(ts, "%Y%m%dT%H%M%S"), ".parquet"))
write_parquet(df, pq_path)
cat("Wrote:", pq_path, "\n")
