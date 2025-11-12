suppressPackageStartupMessages({
  library(sf); library(dplyr); library(fs); library(purrr)
})

daily_dir <- "data/daily"
files <- dir_ls(daily_dir, glob = "polygons_*.geojson")
if (!length(files)) stop("No polygon files found in ", daily_dir)

message("Reading ", length(files), " polygon files…")
polys_list <- map(files, ~ st_read(.x, quiet = TRUE))

polys <- do.call(rbind, polys_list)
if (is.na(st_crs(polys))) st_crs(polys) <- 4326
polys <- st_transform(polys, 4326) |> st_make_valid() |> st_zm(drop = TRUE, what = "ZM")

# Build hex grid over extent (tune cellsize for file size vs detail)
bbox <- st_bbox(polys)
hex <- st_make_grid(st_as_sfc(bbox), cellsize = 0.02, square = FALSE) |> st_as_sf()
hex$id <- seq_len(nrow(hex))

# Count frequency (# of snapshots with polygon intersecting a cell)
idx <- st_intersects(hex, polys, sparse = TRUE)
hex$count <- lengths(idx)

# Normalize: top-exposed = 1.0
mx <- max(hex$count, na.rm = TRUE)
hex$score <- if (mx > 0) hex$count / mx else 0

# Output
dir_create("public")
today <- Sys.Date()
out_latest <- "public/hq_outage_exposure_hex.geojson"
out_dated  <- file.path("public", paste0("hq_outage_exposure_hex_", today, ".geojson"))

st_write(hex, out_latest, driver = "GeoJSON", delete_dsn = TRUE, quiet = TRUE)
st_write(hex, out_dated,  driver = "GeoJSON", delete_dsn = TRUE, quiet = TRUE)
message("Wrote: ", out_latest, " and ", out_dated, " (max_count=", mx, ")")
