#' Parquet datasets as DelayedArray objects
#'
#' @description
#' The ParquetArray class is a \link[DelayedArray]{DelayedArray} subclass
#' for representing and operating on a Parquet dataset.
#'
#' All the operations available for \link[DelayedArray]{DelayedArray}
#' objects work on ParquetArray objects.
#'
#' @inheritParams ParquetArraySeed
#'
#' @author Patrick Aboyoun
#'
#' @examples
#' # Create a data.frame from the Titanic data
#' df <- do.call(expand.grid, c(dimnames(Titanic), stringsAsFactors = FALSE))
#' df$fate <- Titanic[as.matrix(df)]
#'
#' # Write data to a parquet file
#' tf <- tempfile()
#' on.exit(unlink(tf))
#' arrow::write_parquet(df, tf)
#'
#' pqarray <- ParquetArray(tf, key = c("Class", "Sex", "Age", "Survived"), fact = "fate")
#'
#' @aliases
#' ParquetArray-class
#' [,ParquetArray,ANY,ANY,ANY-method
#' aperm,ParquetArray-method
#' t,ParquetArray-method
#' Ops,ParquetArray,ParquetArray-method
#' Ops,ParquetArray,atomic-method
#' Ops,atomic,ParquetArray-method
#' Math,ParquetArray-method
#'
#' @seealso
#' \code{\link{ParquetArraySeed}},
#' \code{\link[DelayedArray]{DelayedArray}}
#'
#' @include duckdb_connection.R
#' @include ParquetArraySeed.R
#'
#' @name ParquetArray
NULL

#' @export
#' @importClassesFrom DelayedArray DelayedArray
setClass("ParquetArray", contains = "DelayedArray", slots = c(seed = "ParquetArraySeed"))

#' @export
setMethod("duckdb_connection", "ParquetArray", function(x) callGeneric(x@seed))

#' @export
setMethod("[", "ParquetArray", function(x, i, j, ..., drop = TRUE) {
    Nindex <- S4Arrays:::extract_Nindex_from_syscall(sys.call(), parent.frame())
    initialize(x, seed = .subset_ParquetArraySeed(x@seed, Nindex = Nindex, drop = drop))
})

#' @export
#' @importFrom BiocGenerics aperm
setMethod("aperm", "ParquetArray", function(a, perm, ...) {
    initialize(a, seed = aperm(a@seed, perm = perm, ...))
})

#' @export
#' @importFrom BiocGenerics t
setMethod("t", "ParquetArray", function(x) {
    initialize(x, seed = t(x@seed))
})

#' @export
setMethod("Ops", c(e1 = "ParquetArray", e2 = "ParquetArray"), function(e1, e2) {
    initialize(e1, seed = callGeneric(e1@seed, e2@seed))
})

#' @export
setMethod("Ops", c(e1 = "ParquetArray", e2 = "atomic"), function(e1, e2) {
    initialize(e1, seed = callGeneric(e1@seed, e2))
})

#' @export
setMethod("Ops", c(e1 = "atomic", e2 = "ParquetArray"), function(e1, e2) {
    initialize(e2, seed = callGeneric(e1, e2@seed))
})

#' @export
setMethod("Math", "ParquetArray", function(x) {
    initialize(x, seed = callGeneric(x@seed))
})

#' @export
#' @importFrom S4Vectors new2
#' @rdname ParquetArray
ParquetArray <- function(conn, key, fact, type = NULL, ...) {
    if (!is(conn, "ParquetArraySeed")) {
        conn <- ParquetArraySeed(conn, key = key, fact = fact, type = type, ...)
    }
    new2("ParquetArray", seed = conn, check = FALSE)
}
