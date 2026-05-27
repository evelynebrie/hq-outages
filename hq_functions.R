# ----------------------------
# Hydro-Québec outages (R) - IMPROVED VERSION
# Better handling for large datasets
# ----------------------------

library(httr)
library(jsonlite)
library(dplyr)
library(tidyr)
library(stringr)
library(lubridate)

suppressWarnings({
  has_sf <- requireNamespace("sf", quietly = TRUE)
})

BASE        <- "https://pannes.hydroquebec.com/pannes/donnees/v3_0/"  # retired upstream
ARCGIS_BASE <- "https://services5.arcgis.com/0akaykIdiPuMhFIy/arcgis/rest/services/bs_infoPannes_prd_vue/FeatureServer"
TZ          <- "America/Toronto"

# ---------- Helpers ----------

# GET + parse JSON with retries and better timeout handling
get_json <- function(url, retries = 3, timeout_sec = 60) {
  ua <- httr::user_agent("HQ-outages-R/1.0 (+https://example)")
  
  for (i in seq_len(retries)) {
    resp <- try(
      httr::GET(url, ua, httr::timeout(timeout_sec)),
      silent = TRUE
    )
    
    if (inherits(resp, "response") && httr::status_code(resp) == 200) {
      txt <- httr::content(resp, as = "text", encoding = "UTF-8")
      
      # Check if response is too large
      txt_size <- nchar(txt)
      if (txt_size > 50e6) {  # 50MB threshold
        warning("Large JSON response (", format(txt_size, big.mark = ","), 
                " chars). This may cause memory issues.")
      }
      
      # Parse with streaming for very large responses
      if (txt_size > 10e6) {
        # Use stream_in for large responses
        parsed <- try(
          jsonlite::fromJSON(txt, simplifyVector = FALSE),
          silent = TRUE
        )
      } else {
        parsed <- try(
          jsonlite::fromJSON(txt, simplifyVector = FALSE),
          silent = TRUE
        )
      }
      
      if (!inherits(parsed, "try-error")) return(parsed)
    }
    
    # Exponential backoff with jitter
    wait_time <- min(2^i + runif(1, 0, 1), 10)
    message("Retry ", i, " failed, waiting ", round(wait_time, 1), " seconds...")
    Sys.sleep(wait_time)
  }
  
  stop("Failed to fetch or parse JSON from: ", url, 
       "\nLast status: ", 
       if (inherits(resp, "response")) httr::status_code(resp) else "connection error")
}

# Extract 'version' with better error handling
extract_version <- function(x) {
  if (is.null(x)) return(NA_character_)
  
  v <- NULL
  if (is.list(x)) {
    v <- x$version %||na% x[["version"]]
    if (is.null(v) && length(x) >= 1) v <- x[[1]]
  } else if (is.atomic(x)) {
    nm <- names(x)
    if (!is.null(nm) && "version" %in% nm) {
      v <- x[["version"]]
    } else if (length(x) >= 1) {
      v <- x[[1]]
    }
  }
  
  v <- as.character(v)
  if (length(v) == 0) return(NA_character_)
  v[1]
}

# Null/empty coalesce
`%||na%` <- function(a, b) {
  if (is.null(a)) return(b)
  if (is.atomic(a) || is.list(a)) {
    if (length(a) == 0) return(b)
    return(a)
  }
  b
}

# Vectorized, robust datetime parser
parse_dt <- function(x, tz = TZ) {
  x <- as.character(x %||na% character())
  if (!length(x)) return(as.POSIXct(NA, tz = tz))
  
  res <- rep(as.POSIXct(NA, tz = tz), length(x))
  
  # Numeric epoch
  nx <- suppressWarnings(as.numeric(x))
  idx_epoch <- which(!is.na(nx))
  if (length(idx_epoch)) {
    ep <- nx[idx_epoch]
    ep <- ifelse(ep > 1e12, ep / 1000, ep)
    res[idx_epoch] <- as.POSIXct(ep, origin = "1970-01-01", tz = tz)
  }
  
  # String dates
  parsed <- suppressWarnings(lubridate::parse_date_time(
    x,
    orders = c("ymd HMS", "ymd HM", "ymd",
               "Ymd HMS", "Ymd HM",
               "dmY HMS", "dmY HM"),
    tz = tz,
    quiet = TRUE
  ))
  fill_idx <- is.na(res) & !is.na(parsed)
  res[fill_idx] <- parsed[fill_idx]
  res
}

# Extract lon/lat from a string
parse_coords <- function(s) {
  s <- as.character(s)
  nums <- stringr::str_extract_all(s, "-?\\d+\\.?\\d*")
  lon <- lat <- rep(NA_real_, length(nums))
  for (i in seq_along(nums)) {
    if (length(nums[[i]]) >= 2) {
      lon[i] <- suppressWarnings(as.numeric(nums[[i]][1]))
      lat[i] <- suppressWarnings(as.numeric(nums[[i]][2]))
    }
  }
  list(lon = lon, lat = lat)
}

# ---------- Main: outages with better memory handling ----------

get_hq_outages <- function(return_sf = FALSE) {
  # Upstream migrated from bismarkers{ver}.json to an ArcGIS FeatureServer.
  # Layer 0 = Marqueurs (point per outage). Schema is much sparser now;
  # fields not exposed publicly (customers, eta_at, status_code, cause_code,
  # municipality_id, etc.) are filled with NA to keep the output schema stable.
  page_size <- 2000
  offset <- 0
  feats <- list()
  repeat {
    url <- sprintf(
      "%s/0/query?where=1%%3D1&outFields=*&f=geojson&resultOffset=%d&resultRecordCount=%d",
      ARCGIS_BASE, offset, page_size
    )
    message("Fetching outages page (offset=", offset, ")")
    payload <- get_json(url, timeout_sec = 60)
    page_feats <- payload$features %||na% list()
    if (length(page_feats) == 0) break
    feats <- c(feats, page_feats)
    if (!isTRUE(payload$properties$exceededTransferLimit)) break
    offset <- offset + page_size
  }

  if (length(feats) == 0) {
    message("No outages in payload.")
    return(tibble::tibble())
  }

  message("Processing ", length(feats), " outages...")

  df <- lapply(feats, function(f) {
    props  <- f$properties %||na% list()
    coords <- f$geometry$coordinates %||na% list(NA_real_, NA_real_)
    tibble::tibble(
      customers        = NA_integer_,
      started_at_raw   = as.character(props$dateCreation %||na% NA),
      eta_raw          = NA_character_,
      status_code      = NA_character_,
      lon              = suppressWarnings(as.numeric(coords[[1]])),
      lat              = suppressWarnings(as.numeric(coords[[2]])),
      status2          = NA_character_,
      unknown7         = NA_character_,
      cause_code       = NA_integer_,
      municipality_id  = NA_integer_,
      message_id       = NA_integer_,
      extra11          = NA_character_,
      extra12          = NA_character_,
      idInterruption   = as.character(props$idInterruption %||na% NA),
      panne_majeure    = suppressWarnings(as.integer(props$panneMajeure %||na% NA))
    )
  }) |> dplyr::bind_rows()
  
  # Add metadata
  status_map <- c(
    "A" = "Assigned",
    "L" = "Crew working",
    "R" = "Crew en route",
    "P" = "Outage reported"
  )
  
  df <- df |>
    mutate(
      started_at = parse_dt(started_at_raw),
      eta_at     = parse_dt(eta_raw),
      status     = dplyr::recode(status_code, !!!status_map, .default = status_code),
      cause_group = dplyr::case_when(
        cause_code %in% 21:26 ~ "Weather",
        cause_code %in% 51:59 ~ "Vegetation / Animals",
        TRUE ~ NA_character_
      )
    ) |>
    select(-started_at_raw, -eta_raw)
  
  message("Processed ", nrow(df), " outages successfully")
  
  if (isTRUE(return_sf)) {
    if (!has_sf) stop("Package 'sf' not installed. Install it or call with return_sf = FALSE.")
    df <- df |> filter(!is.na(lon), !is.na(lat))
    message("Converting to spatial format (", nrow(df), " valid points)")
    df <- sf::st_as_sf(df, coords = c("lon", "lat"), crs = 4326)
  }
  
  df
}

# ---------- Polygons ----------

.hq_require_pkg <- function(pkg, why = "") {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    stop(sprintf("Package '%s' is required%s. Install it with install.packages('%s')",
                 pkg, if (nzchar(why)) paste0(" for ", why) else "", pkg))
  }
}

.hq_clean_polys <- function(x) {
  .hq_require_pkg("sf", "polygon cleaning")
  if (is.na(sf::st_crs(x))) sf::st_crs(x) <- 4326
  if (!identical(sf::st_crs(x)$epsg, 4326L)) x <- sf::st_transform(x, 4326)
  x <- suppressWarnings(sf::st_make_valid(x))
  gtypes <- sf::st_geometry_type(x, by_geometry = TRUE)
  if (all(grepl("POLYGON", toupper(gtypes)))) {
    x <- suppressWarnings(sf::st_cast(x, "MULTIPOLYGON"))
  }
  x
}

get_hq_polygons <- function(add_metadata = TRUE, quiet = TRUE) {
  .hq_require_pkg("sf", "polygons output")

  # Upstream now serves polygons as GeoJSON from ArcGIS layer 1 (no more KMZ).
  page_size <- 2000
  offset <- 0
  poly_list <- list()
  repeat {
    url <- sprintf(
      "%s/1/query?where=1%%3D1&outFields=*&f=geojson&resultOffset=%d&resultRecordCount=%d",
      ARCGIS_BASE, offset, page_size
    )
    message("Fetching polygons page (offset=", offset, ")")
    tmp <- tempfile(fileext = ".geojson")
    r <- httr::GET(url,
                   httr::user_agent("HQ-outages-R/1.0"),
                   httr::write_disk(tmp, overwrite = TRUE),
                   httr::timeout(120))
    if (httr::status_code(r) != 200) {
      stop("Failed to fetch polygons: HTTP ", httr::status_code(r))
    }

    chunk <- try(sf::st_read(tmp, quiet = quiet), silent = TRUE)
    if (inherits(chunk, "try-error") || nrow(chunk) == 0) break
    poly_list[[length(poly_list) + 1]] <- chunk

    meta <- try(jsonlite::fromJSON(tmp, simplifyVector = FALSE), silent = TRUE)
    exceeded <- !inherits(meta, "try-error") &&
                isTRUE(meta$properties$exceededTransferLimit)
    if (!exceeded) break
    offset <- offset + page_size
  }

  if (length(poly_list) == 0) {
    stop("No polygons returned from ArcGIS FeatureServer")
  }

  poly <- do.call(rbind, poly_list)
  poly <- .hq_clean_polys(poly)

  # Ensure a poly_id column the scraper's ID-selection loop will find first.
  if ("idInterruption" %in% names(poly)) {
    poly$poly_id <- as.character(poly$idInterruption)
  } else {
    poly$poly_id <- as.character(seq_len(nrow(poly)))
  }

  if (isTRUE(add_metadata)) {
    TZ_local <- if (exists("TZ", inherits = FALSE)) get("TZ") else "America/Toronto"
    poly$hq_version      <- format(Sys.time(), "%Y%m%dT%H%M%S")
    poly$hq_retrieved_at <- as.character(lubridate::with_tz(Sys.time(), TZ_local))
  }

  message("Loaded ", nrow(poly), " polygons")
  poly
}

# ---------- Improved join with chunking ----------

hq_outages_join_polygons <- function(polys_sf = NULL, outages_sf = NULL, 
                                     chunk_size = 1000) {
  .hq_require_pkg("sf", "spatial join")
  
  if (is.null(polys_sf))   polys_sf   <- get_hq_polygons()
  if (is.null(outages_sf)) outages_sf <- get_hq_outages(return_sf = TRUE)
  
  # Choose polygon ID column
  poly_id <- NULL
  for (cand in c("poly_id","id","ID","name","NOM","MUNICIPALITE")) {
    if (cand %in% names(polys_sf)) { 
      poly_id <- cand
      break 
    }
  }
  if (is.null(poly_id)) {
    polys_sf$poly_id <- seq_len(nrow(polys_sf))
    poly_id <- "poly_id"
  }
  
  n_outages <- nrow(outages_sf)
  message("Joining ", n_outages, " outages to ", nrow(polys_sf), " polygons...")
  
  # Use chunking for large datasets
  if (n_outages > chunk_size) {
    n_chunks <- ceiling(n_outages / chunk_size)
    message("Using chunked processing (", n_chunks, " chunks)...")
    
    joined_list <- vector("list", n_chunks)
    
    for (i in seq_len(n_chunks)) {
      start_idx <- (i - 1) * chunk_size + 1
      end_idx <- min(i * chunk_size, n_outages)
      
      message("  Chunk ", i, "/", n_chunks, " (rows ", start_idx, "-", end_idx, ")")
      
      chunk_sf <- outages_sf[start_idx:end_idx, ]
      joined_list[[i]] <- sf::st_join(chunk_sf, polys_sf, 
                                       join = sf::st_within, left = FALSE)
      
      if (i %% 5 == 0) gc()
    }
    
    message("  Combining chunks...")
    joined <- do.call(rbind, joined_list)
    rm(joined_list)
    gc()
    
  } else {
    joined <- sf::st_join(outages_sf, polys_sf, join = sf::st_within, left = FALSE)
  }
  
  # Create summary
  joined_df <- sf::st_drop_geometry(joined)
  
  if ("customers" %in% names(joined_df)) {
    summary <- joined_df |>
      dplyr::group_by(.data[[poly_id]]) |>
      dplyr::summarise(
        n_outages    = dplyr::n(),
        customers_sum = sum(customers, na.rm = TRUE),
        customers_mean = mean(customers, na.rm = TRUE),
        .groups = "drop"
      )
  } else {
    summary <- joined_df |>
      dplyr::group_by(.data[[poly_id]]) |>
      dplyr::summarise(
        n_outages = dplyr::n(),
        .groups = "drop"
      )
  }
  
  # Join back to polygons
  polys_aug <- polys_sf |>
    dplyr::mutate(poly_key = .data[[poly_id]]) |>
    dplyr::left_join(
      { tmp <- summary; names(tmp)[names(tmp) == poly_id] <- "poly_key"; tmp },
      by = "poly_key"
    ) |>
    dplyr::select(-poly_key)
  
  message("Join complete: ", nrow(summary), " polygons have outages")
  
  list(
    polygons      = polys_aug,
    joined_points = joined,
    summary       = summary
  )
}

# ---------- Optional leaflet map ----------

hq_leaflet_map <- function(polys_sf = NULL, outages_sf = NULL,
                           polygon_weight = 1, polygon_opacity = 0.25, 
                           point_radius = 5) {
  .hq_require_pkg("leaflet", "interactive map")
  if (is.null(polys_sf))   polys_sf   <- get_hq_polygons()
  if (is.null(outages_sf)) outages_sf <- get_hq_outages(return_sf = TRUE)
  
  # Limit display for very large datasets
  if (nrow(outages_sf) > 10000) {
    warning("Too many outages (", nrow(outages_sf), ") for interactive map. ",
            "Sampling 10,000 for display.")
    outages_sf <- outages_sf[sample(nrow(outages_sf), 10000), ]
  }
  
  # Simple popups
  poly_popup_fields <- intersect(
    c("hq_version","hq_retrieved_at","name","NOM","MUNICIPALITE","poly_id","id","ID"),
    names(polys_sf)
  )
  poly_popup <- if (length(poly_popup_fields)) {
    apply(as.data.frame(polys_sf[poly_popup_fields], stringsAsFactors = FALSE), 1, 
          function(r) {
            paste(paste0("<b>", names(r), ":</b> ", r), collapse = "<br/>")
          })
  } else NULL
  
  pt_fields <- intersect(
    c("customers","status","cause_group","started_at","eta_at"), 
    names(outages_sf)
  )
  pt_popup <- if (length(pt_fields)) {
    apply(as.data.frame(sf::st_drop_geometry(outages_sf)[pt_fields], 
                       stringsAsFactors = FALSE), 1, function(r) {
      paste(paste0("<b>", names(r), ":</b> ", r), collapse = "<br/>")
    })
  } else NULL
  
  leaflet::leaflet() |>
    leaflet::addTiles() |>
    leaflet::addPolygons(
      data = polys_sf,
      weight = polygon_weight,
      fillOpacity = polygon_opacity,
      popup = poly_popup
    ) |>
    leaflet::addCircleMarkers(
      data = outages_sf,
      radius = point_radius,
      stroke = FALSE,
      fillOpacity = 0.9,
      popup = pt_popup
    )
}
