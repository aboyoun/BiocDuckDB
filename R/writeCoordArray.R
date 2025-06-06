#' Write an Array-Like Object in Coordinate Format
#'
#' @description
#' An \code{arrow::write_dataset} wrapper function to write array-like objects
#' in coordinate format. If dimension names are present, they are substituted
#' for the corresponding indices.
#'
#' @param x An array-like object.
#' @param path The path to write the array-like object to.
#' @param keycols A character vector of column names for the dimensions of the
#' array in the resulting table.
#' @param datacol The name for the column containing the array values in the
#' resulting table.
#' @param dimtbls An optional list of data.frame or DataFrame objects that
#' define the partitioning in \code{arrow::write_dataset}. If specified, the
#' list must have the same length as the number of dimensions of the array and
#' the elements must have rownames that match the corresponding dimnames
#' element.
#' @param partitioning A character vector of column names for the partitions of
#' the array in the resulting table.
#' @param ... Additional arguments to pass to \code{arrow::write_dataset}.
#'
#' @author Patrick Aboyoun
#'
#' @examples
#' # Write the Titanic dataset to a single Parquet file
#' tf1 <- tempfile()
#' writeCoordArray(Titanic, tf1)
#' list.files(tf1, full.names = TRUE, recursive = TRUE)
#'
#' # Write the Titanic dataset to a single csv file
#' tf2 <- tempfile()
#' writeCoordArray(Titanic, tf2, format = "csv")
#' list.files(tf2, full.names = TRUE, recursive = TRUE)
#'
#' # Write the state.x77 matrix to multiple Parquet files
#' tf3 <- tempfile()
#' dimtbls <- list(data.frame(region = state.region,
#'                            division = state.division,
#'                            row.names = state.name),
#'                 NULL)
#' writeCoordArray(state.x77, tf3, dimtbls = dimtbls)
#' list.files(tf3, full.names = TRUE, recursive = TRUE)
#'
#' @keywords IO
#'
#' @name writeCoordArray

#' @export
#' @importFrom arrow write_dataset
#' @importFrom SparseArray nzwhich nzvals
#' @importFrom stats setNames
#' @rdname writeCoordArray
writeCoordArray <-
function(x,
         path,
         keycols = names(dimnames(x)) %||% sprintf("dim%d", seq_along(dim(x))),
         datacol = "value",
         dimtbls = NULL,
         partitioning = unlist(lapply(dimtbls, colnames), use.names = FALSE),
         ...)
{
    dim_x <- dim(x)

    if (is.null(dim_x)) {
        stop("'x' must be an array-like object")
    } else if (inherits(x, "table")) {
        x <- unclass(x)
    }

    # Make column names unique
    unique_names <- make.unique(c(keycols, datacol), sep = "_")
    keycols <- head(unique_names, -1L)
    datacol <- tail(unique_names, 1L)

    # Create a list of columns containing the non-zero values and their indices
    lst <- apply(nzwhich(x, arr.ind = TRUE), 2L, identity, simplify = FALSE)
    names(lst) <- keycols
    lst[[datacol]] <- nzvals(x)

    # Add the partitioning columns, if any
    for (j in seq_along(dimtbls)) {
        tbl <- dimtbls[[j]]
        if (NROW(tbl)) {
            if (length(dim(tbl)) != 2L) {
                stop("'dimtbls' must have two dimensions")
            }
            if (is.null(rownames(tbl))) {
                stop("rownames must be defined for each element of 'dimtbls'")
            }
            if (is.null(dimnames(x)[[j]])) {
                stop("dimnames must be defined for each dimension of 'x' when 'dimtbls' is specified")
            }
            if (!identical(rownames(tbl), dimnames(x)[[j]])) {
                tbl <- tbl[dimnames(x)[[j]], , drop = FALSE]
            }
            lst <- c(lst, sapply(tbl, function(z) z[lst[[j]]], simplify = FALSE))
       }
    }

    # Add the dimnames, if any
    dimnames_x <- dimnames(x)
    for (j in seq_along(dimnames_x)) {
        dimnames_x_j <- dimnames_x[[j]]
        if (!is.null(dimnames_x_j)) {
            lst[[j]] <- dimnames_x_j[lst[[j]]]
        }
    }

    class(lst) <- "data.frame"
    attr(lst, "row.names") <- .set_row_names(length(lst[[1L]]))

    write_dataset(lst, path, partitioning = partitioning, ...)
}
