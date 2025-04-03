#' DuckDBArray objects
#'
#' @description
#' The DuckDBArray class is a \link[DelayedArray]{DelayedArray} subclass
#' for representing and operating on a DuckDB table.
#'
#' All the operations available for \link[DelayedArray]{DelayedArray}
#' objects work on DuckDBArray objects.
#'
#' @section Constructor:
#' \describe{
#'   \item{\code{DuckDBArray(conn, datacol, keycols, dimtbls = NULL, type = NULL)}:}{
#'     Creates a DuckDBArray object.
#'     \describe{
#'       \item{\code{conn}}{
#'         Either a character vector containing the paths to parquet, csv, or
#'         gzipped csv data files; a string that defines a duckdb \code{read_*}
#'         data source; a DuckDBDataFrame object; or a tbl_duckdb_connection
#'         object.
#'       }
#'       \item{\code{datacol}}{
#'         Either a string specifying the column from \code{conn} or a named
#'         \code{expression} that will be evaluated in the context of
#'         \code{conn} that defines the values in the array.
#'       }
#'       \item{\code{keycols}}{
#'         Either a character vector of column names from \code{conn} that will
#'         specify the dimension names, or a named list of character vectors
#'         where the names of the list specify the dimension names and the
#'         character vectors set the distinct values for the dimension names.
#'       }
#'       \item{\code{dimtbls}}{
#'         A optional named \code{DataFrameList} that specifies the dimension
#'         tables associated with the \code{keycols}. The name of the list
#'         elements match the names of the \code{keycols} list. Additionally,
#'         the \code{DataFrame} objects have row names that match the distinct
#'         values of the corresponding \code{keycols} list element and columns
#'         that define partitions in the data table for efficient querying.
#'       }
#'       \item{\code{type}}{
#'         String specifying the type of the data values; one of
#'         \code{"logical"}, \code{"integer"}, \code{"integer64"},
#'         \code{"double"}, or \code{"character"}. If \code{NULL}, it is
#'         determined by inspecting the data.
#'       }
#'     }
#'   }
#' }
#'
#' @section Accessors:
#' In the code snippets below, \code{x} is a DuckDBArray object:
#' \describe{
#'   \item{\code{dim(x)}:}{
#'     An integer vector of the array dimensions.
#'   }
#'   \item{\code{dimnames(x)}:}{
#'     List of array dimension names.
#'   }
#'   \item{\code{dimtbls(x)}, \code{dimtbls(x) <- value}:}{
#'     Get or set the list of dimension tables used to define partitions for
#'     efficient queries.
#'   }
#'   \item{\code{type(x)}, \code{type(x) <- value}:}{
#'     Get or set the data type of the array elements; one of \code{"logical"},
#'     \code{"integer"}, \code{"integer64"}, \code{"double"}, or
#'     \code{"character"}.
#'   }
#' }
#'
#' @section Subsetting:
#' In the code snippets below, \code{x} is a DuckDBArray object:
#' \describe{
#'   \item{\code{x[i, j, ..., drop = TRUE]}:}{
#'     Returns a new DuckDBArray object. Empty dimensions are dropped if
#'     \code{drop = TRUE}.
#'   }
#' }
#'
#' @section Transposition:
#' In the code snippets below, \code{x} is a DuckDBArray object:
#' \describe{
#'   \item{\code{aperm(a, perm)}:}{
#'     Returns a new DuckDBArray object with the dimensions permuted
#'     according to the \code{perm} vector.
#'   }
#'   \item{\code{t(x)}:}{
#'     For two-dimensional arrays, returns a new DuckDBArray object with the
#'     dimensions transposed.
#'   }
#' }
#'
#' @author Patrick Aboyoun
#'
#' @examples
#' # Create a data.frame from the Titanic data
#' df <- do.call(expand.grid, c(dimnames(Titanic), stringsAsFactors = FALSE))
#' df$fate <- Titanic[as.matrix(df)]
#'
#' # Write data to a parquet file
#' tf <- tempfile(fileext = ".parquet")
#' on.exit(unlink(tf))
#' arrow::write_parquet(df, tf)
#'
#' pqarray <- DuckDBArray(tf, datacol = "fate", keycols = c("Class", "Sex", "Age", "Survived"))
#'
#' @aliases
#' DuckDBArray-class
#'
#' dbconn,DuckDBArray-method
#' tblconn,DuckDBArray-method
#' dimtbls,DuckDBArray-method
#' dimtbls<-,DuckDBArray-method
#' type,DuckDBArray-method
#' type<-,DuckDBArray-method
#'
#' DuckDBArray
#'
#' [,DuckDBArray,ANY,ANY,ANY-method
#'
#' aperm,DuckDBArray-method
#' t,DuckDBArray-method
#'
#' @seealso
#' \code{\link{DuckDBArraySeed}},
#' \code{\link[DelayedArray]{DelayedArray}}
#'
#' @include DuckDBArraySeed-class.R
#' @include DuckDBArraySeed-utils.R
#'
#' @keywords classes methods
#'
#' @name DuckDBArray-class
NULL

#' @export
#' @importClassesFrom DelayedArray DelayedArray
setClass("DuckDBArray", contains = "DelayedArray", slots = c(seed = "DuckDBArraySeed"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

#' @export
#' @importFrom BiocGenerics dbconn
setMethod("dbconn", "DuckDBArray", function(x) callGeneric(x@seed))

#' @export
setMethod("tblconn", "DuckDBArray", function(x, select = TRUE, filter = TRUE) {
    callGeneric(x@seed, select = select, filter = filter)
})

#' @export
setMethod("dimtbls", "DuckDBArray", function(x) callGeneric(x@seed))

#' @export
setReplaceMethod("dimtbls", "DuckDBArray", function(x, value) {
    callGeneric(x@seed, value)
})

#' @export
#' @importFrom BiocGenerics type
setMethod("type", "DuckDBArray", function(x) callGeneric(x@seed))

#' @export
#' @importFrom BiocGenerics type<-
setReplaceMethod("type", "DuckDBArray", function(x, value) {
    replaceSlots(x, seed = callGeneric(x@seed, value = value), check = FALSE)
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

#' @export
#' @importFrom S4Vectors new2
DuckDBArray <- function(conn, datacol, keycols, dimtbls = NULL, type = NULL) {
    if (!is(conn, "DuckDBArraySeed")) {
        conn <- DuckDBArraySeed(conn, datacol = datacol, keycols = keycols,
                                dimtbls = dimtbls, type = type)
    }
    new2("DuckDBArray", seed = conn, check = FALSE)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Subsetting
###

#' @export
setMethod("[", "DuckDBArray", function(x, i, j, ..., drop = TRUE) {
    Nindex <- S4Arrays:::extract_Nindex_from_syscall(sys.call(), parent.frame())
    replaceSlots(x, seed = .subset_DuckDBArraySeed(x@seed, Nindex = Nindex, drop = drop), check = FALSE)
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Transposition
###

#' @export
#' @importFrom BiocGenerics aperm
setMethod("aperm", "DuckDBArray", function(a, perm, ...) {
    replaceSlots(a, seed = aperm(a@seed, perm = perm, ...), check = FALSE)
})

#' @export
#' @importFrom BiocGenerics t
setMethod("t", "DuckDBArray", function(x) {
    replaceSlots(x, seed = t(x@seed), check = FALSE)
})
