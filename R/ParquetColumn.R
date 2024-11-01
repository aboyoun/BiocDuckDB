#' ParquetColumn objects
#'
#' @author Patrick Aboyoun
#'
#' @include duckdb_connection.R
#' @include ParquetFactTable.R
#'
#' @aliases
#' ParquetColumn-class
#' as.vector,ParquetColumn-method
#' extractROWS,ParquetColumn,ANY-method
#' head,ParquetColumn-method
#' length,ParquetColumn-method
#' names,ParquetColumn-method
#' show,ParquetColumn-method
#' showAsCell,ParquetColumn-method
#' tail,ParquetColumn-method
#' Ops,ParquetColumn,ParquetColumn-method
#' Ops,ParquetColumn,atomic-method
#' Ops,atomic,ParquetColumn-method
#' Math,ParquetColumn-method
#'
#' @name ParquetColumn
NULL

#' @export
#' @importClassesFrom S4Vectors Vector
setClass("ParquetColumn", contains = "Vector", slots = c(table = "ParquetFactTable"))

#' @importFrom S4Vectors isTRUEorFALSE setValidity2
setValidity2("ParquetColumn", function(x) {
    table <- x@table
    if (ncol(table) != 1L) {
        return("'table' slot must be a single-column ParquetFactTable")
    }
    if (nkey(table) != 1L) {
        return("'table' slot must have a 'key' with a named list containing a single named character vector")
    }
    TRUE
})

#' @export
#' @importFrom S4Vectors classNameForDisplay
setMethod("show", "ParquetColumn", function(object) {
    len <- length(object)
    cat(sprintf("%s of length %d\n", classNameForDisplay(object), len))
    n1 <- n2 <- 2L
    if (len <= n1 + n2 + 1L) {
        vec <- as.vector(object)
    } else {
        i <- c(seq_len(n1), (len + 1L) - rev(seq_len(n2)))
        vec <- as.vector(object[i])
        if (is.character(vec)) {
            vec <- setNames(sprintf("\"%s\"", vec), names(vec))
        }
        vec <- format(vec, justify = "right")
        vec1 <- head(vec, n1)
        vec2 <- tail(vec, n2)
        vec <- c(vec1, "..." = "...", vec2)
    }
    print(vec, quote = FALSE)
})

#' @export
#' @importFrom S4Vectors showAsCell
setMethod("showAsCell", "ParquetColumn", function(object) {
    callGeneric(as.vector(object@table))
})

#' @export
setMethod("duckdb_connection", "ParquetColumn", function(x) callGeneric(x@table))

#' @export
setMethod("length", "ParquetColumn", function(x) nrow(x@table))

#' @export
setMethod("names", "ParquetColumn", function(x) keydimnames(x@table)[[1L]])

#' @export
setMethod("extractROWS", "ParquetColumn", function(x, i) {
    if (is(i, "ParquetColumn")) {
        i <- i@table
    }
    i <- setNames(list(i), keynames(x@table))
    initialize(x, table = .subset_ParquetFactTable(x@table, i = i))
})

#' @export
#' @importFrom S4Vectors head isSingleNumber
setMethod("head", "ParquetColumn", function(x, n = 6L, ...) {
    if (!isSingleNumber(n)) {
        stop("'n' must be a single number")
    }
    n <- as.integer(n)
    len <- length(x)
    if (n < 0) {
        n <- max(0L, len + n)
    }
    if (n > len) {
        x
    } else {
        extractROWS(x, seq_len(n))
    }
})

#' @export
#' @importFrom S4Vectors isSingleNumber tail
setMethod("tail", "ParquetColumn", function(x, n = 6L, ...) {
    if (!isSingleNumber(n)) {
        stop("'n' must be a single number")
    }
    n <- as.integer(n)
    len <- length(x)
    if (n < 0) {
        n <- max(0L, len + n)
    }
    if (n > len) {
        x
    } else {
        extractROWS(x, (len + 1L) - rev(seq_len(n)))
    }
})

#' @export
setMethod("Ops", c(e1 = "ParquetColumn", e2 = "ParquetColumn"), function(e1, e2) {
    initialize(e1, table = callGeneric(e1@table, e2@table))
})

#' @export
setMethod("Ops", c(e1 = "ParquetColumn", e2 = "atomic"), function(e1, e2) {
    initialize(e1, table = callGeneric(e1@table, e2))
})

#' @export
setMethod("Ops", c(e1 = "atomic", e2 = "ParquetColumn"), function(e1, e2) {
    initialize(e1, table = callGeneric(e1, e2@table))
})

#' @export
setMethod("Math", "ParquetColumn", function(x) {
    initialize(x, table = callGeneric(x@table))
})

#' @export
#' @importFrom BiocGenerics as.vector
#' @importFrom stats setNames
setMethod("as.vector", "ParquetColumn", function(x, mode = "any") {
    df <- as.data.frame(x@table)
    vec <- setNames(df[[colnames(x@table)]], df[[keynames(x@table)[[1L]]]])
    vec <- vec[rownames(x@table)]
    if (mode != "any") {
        storage.mode(vec) <- mode
    }
    vec
})
