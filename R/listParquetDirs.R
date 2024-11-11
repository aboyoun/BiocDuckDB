#' List the Directories Containing Parquet or CSV Files
#'
#' @description
#' A \code{list.files} wrapper function to list directories containing parquet
#' or csv files.
#'
#' @param path A character vector specifying the top-level directories.
#'
#' @author Patrick Aboyoun
#'
#' @examples
#' # Create a state dataset
#' state <- data.frame(
#'   region = rep(as.character(state.region), times = ncol(state.x77)),
#'   division = rep(as.character(state.division), times = ncol(state.x77)),
#'   rowname = rep(rownames(state.x77), times = ncol(state.x77)),
#'   colname = rep(colnames(state.x77), each = nrow(state.x77)),
#'   value = as.vector(state.x77)
#' )
#'
#' # Write a set of parquet files to contain state data
#' state_pq <- tempfile()
#' arrow::write_dataset(state, state_pq, format = "parquet", partitioning = c("region", "division"))
#' listParquetDirs(state_pq)
#'
#' # Write a set of parquet files to contain state data
#' state_csv <- tempfile()
#' arrow::write_dataset(state, state_csv, format = "csv", partitioning = c("region", "division"))
#' listCsvDirs(state_csv)
#'
#' @aliases
#' listCsvDirs
#' listParquetDirs
#'
#' @name listParquetDirs
NULL

#' @export
#' @rdname listParquetDirs
listCsvDirs <- function(path) {
  pattern <- "(?i)\\.(csv|tsv)(\\.gz)?$"
  files <- list.files(path, pattern = pattern, full.names = TRUE, recursive = TRUE)
  sort(unique(dirname(files)))
}

#' @export
#' @rdname listParquetDirs
listParquetDirs <- function(path) {
  pattern <- "(?i)\\.(parquet|pq)$"
  files <- list.files(path, pattern = pattern, full.names = TRUE, recursive = TRUE)
  sort(unique(dirname(files)))
}
