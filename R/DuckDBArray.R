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
#' [,DuckDBArray,ANY,ANY,ANY-method
#' aperm,DuckDBArray-method
#' dbconn,DuckDBArray-method
#' is_nonzero,DuckDBArray-method
#' is.finite,DuckDBArray-method
#' is.infinite,DuckDBArray-method
#' is.nan,DuckDBArray-method
#' nzcount,DuckDBArray-method
#' t,DuckDBArray-method
#' tblconn,DuckDBArray-method
#' type,DuckDBArray-method
#' type<-,DuckDBArray-method
#' Ops,DuckDBArray,DuckDBArray-method
#' Ops,DuckDBArray,atomic-method
#' Ops,atomic,DuckDBArray-method
#' Math,DuckDBArray-method
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

#' @export
#' @importFrom BiocGenerics dbconn
setMethod("dbconn", "DuckDBArray", function(x) callGeneric(x@seed))

#' @export
setMethod("tblconn", "DuckDBArray", function(x) callGeneric(x@seed))

#' @export
setMethod("[", "DuckDBArray", function(x, i, j, ..., drop = TRUE) {
    Nindex <- S4Arrays:::extract_Nindex_from_syscall(sys.call(), parent.frame())
    initialize2(x, seed = .subset_DuckDBArraySeed(x@seed, Nindex = Nindex, drop = drop), check = FALSE)
})

#' @export
#' @importFrom BiocGenerics aperm
setMethod("aperm", "DuckDBArray", function(a, perm, ...) {
    initialize2(a, seed = aperm(a@seed, perm = perm, ...), check = FALSE)
})

#' @export
#' @importFrom BiocGenerics t
setMethod("t", "DuckDBArray", function(x) {
    initialize2(x, seed = t(x@seed), check = FALSE)
})

#' @export
#' @importFrom BiocGenerics type
setMethod("type", "DuckDBArray", function(x) {
    callGeneric(x@seed)
})

#' @export
#' @importFrom BiocGenerics type<-
setReplaceMethod("type", "DuckDBArray", function(x, value) {
    initialize2(x, seed = callGeneric(x@seed, value = value), check = FALSE)
})

#' @export
#' @importFrom SparseArray is_nonzero
setMethod("is_nonzero", "DuckDBArray", function(x) {
    initialize2(x, seed = callGeneric(x@seed), check = FALSE)
})

#' @export
#' @importFrom SparseArray nzcount
setMethod("nzcount", "DuckDBArray", function(x) {
    callGeneric(x@seed)
})

#' @export
setMethod("Ops", c(e1 = "DuckDBArray", e2 = "DuckDBArray"), function(e1, e2) {
    initialize2(e1, seed = callGeneric(e1@seed, e2@seed), check = FALSE)
})

#' @export
setMethod("Ops", c(e1 = "DuckDBArray", e2 = "atomic"), function(e1, e2) {
    initialize2(e1, seed = callGeneric(e1@seed, e2), check = FALSE)
})

#' @export
setMethod("Ops", c(e1 = "atomic", e2 = "DuckDBArray"), function(e1, e2) {
    initialize2(e2, seed = callGeneric(e1, e2@seed), check = FALSE)
})

#' @export
setMethod("Math", "DuckDBArray", function(x) {
    initialize2(x, seed = callGeneric(x@seed), check = FALSE)
})

#' @export
setMethod("is.finite", "DuckDBArray", function(x) {
    initialize2(x, seed = is.finite(x@seed), check = FALSE)
})

#' @export
setMethod("is.infinite", "DuckDBArray", function(x) {
    initialize2(x, seed = is.infinite(x@seed), check = FALSE)
})

#' @export
setMethod("is.nan", "DuckDBArray", function(x) {
    initialize2(x, seed = is.nan(x@seed), check = FALSE)
})

#' @export
#' @importFrom S4Vectors new2
#' @rdname DuckDBArray
DuckDBArray <- function(conn, datacols, keycols, type = NULL) {
    if (!is(conn, "DuckDBArraySeed")) {
        conn <- DuckDBArraySeed(conn, datacols = datacols, keycols = keycols, type = type)
    }
    new2("DuckDBArray", seed = conn, check = FALSE)
}
