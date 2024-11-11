#' Write an Array-Like Object
#'
#' @description
#' An \code{arrow::write_dataset} wrapper function to write array-like objects.
#'
#' @param x An array-like object.
#' @param path The path to write the array-like object to.
#' @param dim_names A character vector of names for the dimensions of the array.
#' @param value_name The name for the column containing the array values.
#' @param partition_size A vector of integers specifying the sub-array size of
#' each partition. The default is to write the entire array as a single
#' partition.
#' @param partition_names A character vector of names for the partitions of the
#' array.
#' @param ... Additional arguments to pass to \code{arrow::write_dataset}.
#'
#' @author Patrick Aboyoun
#'
#' @examples
#' # Write the Titanic dataset to a single Parquet file
#' tf1 <- tempfile()
#' writeArray(Titanic, tf1)
#' list.files(tf1, full.names = TRUE, recursive = TRUE)
#'
#' # Write the Titanic dataset to multiple Parquet files
#' tf2 <- tempfile()
#' writeArray(Titanic, tf2, partition_size = c(2, 2, 2, 2))
#' list.files(tf2, full.names = TRUE, recursive = TRUE)
#'
#' # Write the Titanic dataset to multiple csv files
#' tf3 <- tempfile()
#' writeArray(Titanic, tf3, partition_size = c(2, 2, 2, 2), format = "csv")
#' list.files(tf3, full.names = TRUE, recursive = TRUE)
#'
#' # Write a random array to multiple Parquet files
#' tf4 <- tempfile()
#' X <- randomSparseMatrix(50000, 2^17, density = 0.0001)
#' writeArray(X, tf4, partition_size = c(nrow(X), 100000))
#' files <- list.files(tf4, full.names = TRUE, recursive = TRUE)
#' files <- setNames(sprintf("%.1f MB", file.size(files) / 2^20), files)
#' files
#'
#' @export
#' @importFrom arrow write_dataset
#' @importFrom SparseArray nzwhich nzvals
#' @importFrom stats setNames
#'
#' @rdname writeArray
writeArray <-
function(x,
         path,
         dim_names = names(dimnames(x)) %||% sprintf("dim%d", seq_along(dim(x))),
         value_name = "value",
         partition_size = dim(x),
         partition_names = sprintf("%s_group", dim_names),
         ...)
{
    dim_x <- dim(x)

    if (is.null(dim_x)) {
        stop("'x' must be an array-like object")
    } else if (inherits(x, "table")) {
        x <- unclass(x)
    }

    if (!(length(partition_size) %in% c(1L, length(dim_x)))) {
        stop("'partition_size' must either be of length 1 or the dimensions of 'x'")
    }

    partition_size <- as.integer(partition_size)
    if (length(partition_size) == 1L) {
        partition_size <- rep.int(partition_size, length(dim_x))
    }
    if (anyNA(partition_size) || any(partition_size < 1L)) {
        stop("'partition_size' must be a vector of positive integers")
    }

    # Create a data frame with the non-zero values and their indices
    df <- data.frame(nzwhich(x, arr.ind = TRUE))
    colnames(df) <- dim_names
    df <- do.call(cbind, list(df, setNames(list(nzvals(x)), value_name)))

    # Add the partitioning columns, if any
    partitioning <- setNames(partition_size < dim_x, partition_names)
    for (j in which(partitioning)) {
        k <- ceiling(dim_x[j] / partition_size[j])
        partition <- rep(0:(k - 1L), each = partition_size[j], length.out = dim_x[j])
        df[[partition_names[j]]] <- partition[df[[j]]]
    }
    partitioning <- names(partitioning)[partitioning]

    # Add the dimnames, if any
    dimnames_x <- dimnames(x)
    for (j in seq_along(dimnames_x)) {
        if (!is.null(dimnames_x[[j]])) {
            df[[j]] <- dimnames_x[[j]][df[[j]]]
        }
    }

    write_dataset(df, path, partitioning = partitioning, ...)
}
