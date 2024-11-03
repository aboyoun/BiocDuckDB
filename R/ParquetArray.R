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
#' dbconn,ParquetArray-method
#' is_nonzero,ParquetArray-method
#' nzcount,ParquetArray-method
#' t,ParquetArray-method
#' type,ParquetArray-method
#' type<-,ParquetArray-method
#' Ops,ParquetArray,ParquetArray-method
#' Ops,ParquetArray,atomic-method
#' Ops,atomic,ParquetArray-method
#' Math,ParquetArray-method
#' Summary,ParquetArray-method
#' mean,ParquetArray-method
#' median.ParquetArray
#' quantile.ParquetArray
#' var,ParquetArray,ANY-method
#' sd,ParquetArray-method
#' mad,ParquetArray-method
#'
#' @seealso
#' \code{\link{ParquetArraySeed}},
#' \code{\link[DelayedArray]{DelayedArray}}
#'
#' @include ParquetArraySeed.R
#'
#' @name ParquetArray
NULL

#' @export
#' @importClassesFrom DelayedArray DelayedArray
setClass("ParquetArray", contains = "DelayedArray", slots = c(seed = "ParquetArraySeed"))

#' @export
#' @importFrom BiocGenerics dbconn
setMethod("dbconn", "ParquetArray", function(x) callGeneric(x@seed))

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
#' @importFrom BiocGenerics type
setMethod("type", "ParquetArray", function(x) {
    callGeneric(x@seed)
})

#' @export
#' @importFrom BiocGenerics type<-
setReplaceMethod("type", "ParquetArray", function(x, value) {
    initialize(x, seed = callGeneric(x@seed, value = value))
})

#' @export
#' @importFrom SparseArray is_nonzero
setMethod("is_nonzero", "ParquetArray", function(x) {
    initialize(x, seed = callGeneric(x@seed))
})

#' @export
#' @importFrom SparseArray nzcount
setMethod("nzcount", "ParquetArray", function(x) {
    callGeneric(x@seed)
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
setMethod("Summary", "ParquetArray", function(x, ..., na.rm = FALSE) {
    callGeneric(x@seed)
})

#' @export
#' @importFrom BiocGenerics mean
setMethod("mean", "ParquetArray", function(x, ...) {
    callGeneric(x@seed)
})

#' @exportS3Method stats::median
#' @importFrom stats median
median.ParquetArray <- function(x, na.rm = FALSE, ...) {
    median(x@seed, na.rm = na.rm, ...)
}

#' @exportS3Method stats::quantile
#' @importFrom stats quantile
quantile.ParquetArray <-
function(x, probs = seq(0, 1, 0.25), na.rm = FALSE, names = TRUE, type = 7, digits = 7, ...) {
    quantile(x@seed, probs = probs, na.rm = na.rm, names = names, type = type, digits = digits, ...)
}

#' @export
#' @importFrom BiocGenerics var
setMethod("var", "ParquetArray", function(x, y = NULL, na.rm = FALSE, use)  {
    callGeneric(x@seed)
})

#' @export
#' @importFrom BiocGenerics sd
setMethod("sd", "ParquetArray", function(x, na.rm = FALSE) {
    callGeneric(x@seed)
})

#' @export
#' @importFrom BiocGenerics mad
setMethod("mad", "ParquetArray",
function(x, center = median(x), constant = 1.4826, na.rm = FALSE, low = FALSE, high = FALSE) {
    callGeneric(x@seed)
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
