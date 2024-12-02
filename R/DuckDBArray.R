#' DuckDB tables as DelayedArray objects
#'
#' @description
#' The DuckDBArray class is a \link[DelayedArray]{DelayedArray} subclass
#' for representing and operating on a DuckDB table.
#'
#' All the operations available for \link[DelayedArray]{DelayedArray}
#' objects work on DuckDBArray objects.
#'
#' @inheritParams DuckDBArraySeed
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
#' pqarray <- DuckDBArray(tf, datacols = "fate", keycols = c("Class", "Sex", "Age", "Survived"))
#'
#' @aliases
#' DuckDBArray-class
#'
#' dbconn,DuckDBArray-method
#' tblconn,DuckDBArray-method
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
#' @include DuckDBArraySeed.R
#'
#' @name DuckDBArray
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
setMethod("tblconn", "DuckDBArray", function(x) callGeneric(x@seed))

#' @export
#' @importFrom BiocGenerics type
setMethod("type", "DuckDBArray", function(x) {
    callGeneric(x@seed)
})

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
#' @rdname DuckDBArray
DuckDBArray <- function(conn, datacols, keycols, type = NULL) {
    if (!is(conn, "DuckDBArraySeed")) {
        conn <- DuckDBArraySeed(conn, datacols = datacols, keycols = keycols, type = type)
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
