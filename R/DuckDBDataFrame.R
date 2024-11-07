#' DuckDB-backed DataFrame
#'
#' Create a DuckDB-backed \linkS4class{DataFrame}
#'
#' @inheritParams DuckDBTable
#'
#' @return A DuckDBDataFrame where each column is a \linkS4class{DuckDBColumn}.
#'
#' @details
#' The DuckDBDataFrame is essentially just a \linkS4class{DataFrame} of \linkS4class{DuckDBColumn} objects.
#' It is primarily useful for indicating that the R representation is consistent with the underlying data
#' (e.g., no delayed filter/mutate operations have been applied, no data has been added from other files).
#' Thus, users can specialize code paths for a DuckDBDataFrame to operate directly on the underlying data.
#'
#' @author Patrick Aboyoun, Aaron Lun
#'
#' @examples
#' # Mocking up a file:
#' tf <- tempfile()
#' on.exit(unlink(tf))
#' arrow::write_parquet(cbind(model = rownames(mtcars), mtcars), tf)
#'
#' # Creating our DuckDB-backed data frame:
#' df <- DuckDBDataFrame(tf, keycols = "model")
#' df
#'
#' # Extraction yields a DuckDBColumn:
#' df$carb
#'
#' # Slicing DuckDBDataFrame objects:
#' df[,1:5]
#' df[1:5,]
#'
#' # Combining by DuckDBDataFrame and DuckDBColumn objects:
#' combined <- cbind(df, df)
#' class(combined)
#' combined2 <- cbind(df, some_new_name=df[,1])
#' class(combined2)
#'
#' @aliases
#' DuckDBDataFrame-class
#' makeNakedCharacterMatrixForDisplay,DuckDBDataFrame-method
#' show,DuckDBDataFrame-method
#'
#' length,DuckDBDataFrame-method
#'
#' names,DuckDBDataFrame-method
#' rownames<-,DuckDBDataFrame-method
#' names<-,DuckDBDataFrame-method
#'
#' extractROWS,DuckDBDataFrame,ANY-method
#' head,DuckDBDataFrame-method
#' tail,DuckDBDataFrame-method
#' extractCOLS,DuckDBDataFrame-method
#' [,DuckDBDataFrame,ANY,ANY,ANY-method
#' subset.DuckDBDataFrame
#' subset,DuckDBDataFrame-method
#' [[,DuckDBDataFrame-method
#'
#' replaceROWS,DuckDBDataFrame-method
#' replaceCOLS,DuckDBDataFrame-method
#' normalizeSingleBracketReplacementValue,DuckDBDataFrame-method
#' [[<-,DuckDBDataFrame-method
#'
#' bindROWS,DuckDBDataFrame-method
#' cbind,DuckDBDataFrame-method
#' cbind.DuckDBDataFrame
#'
#' as.data.frame,DuckDBDataFrame-method
#' as.env,DuckDBDataFrame-method
#'
#' @include acquireTable.R
#' @include DuckDBColumn.R
#' @include DuckDBTable.R
#'
#' @name DuckDBDataFrame
NULL

#' @export
#' @importClassesFrom S4Vectors DataFrame
setClass("DuckDBDataFrame", contains = c("DuckDBTable", "DataFrame"))

#' @importFrom S4Vectors setValidity2
setValidity2("DuckDBDataFrame", function(x) {
    if (nkey(x) != 1L) {
        return("'keycols' slot must be a named list containing a single named character vector")
    }
    TRUE
})

#' @export
#' @importFrom S4Vectors DataFrame makeNakedCharacterMatrixForDisplay
setMethod("makeNakedCharacterMatrixForDisplay", "DuckDBDataFrame", function(x) {
    callGeneric(DataFrame(as.data.frame(x)))
})

#' @export
#' @importFrom S4Vectors classNameForDisplay DataFrame get_showHeadLines get_showTailLines makeNakedCharacterMatrixForDisplay
setMethod("show", "DuckDBDataFrame", function(object) {
    x_nrow <- nrow(object)
    x_ncol <- ncol(object)

    cat(classNameForDisplay(object), " with ",
        x_nrow, " row", ifelse(x_nrow == 1L, "", "s"), " and ",
        x_ncol, " column", ifelse(x_ncol == 1L, "", "s"), "\n", sep = "")

    if (.has.row_number(object)) {
        nhead <- get_showHeadLines() + get_showTailLines()
        ntail <- 0L
    } else {
        nhead <- get_showHeadLines()
        ntail <- get_showTailLines()
    }

    if (x_nrow != 0L && x_ncol != 0L) {
        if (x_nrow <= nhead + ntail + 1L) {
            m <- makeNakedCharacterMatrixForDisplay(object)
            x_rownames <- rownames(object)
            if (!is.null(x_rownames)) {
                rownames(m) <- x_rownames
            }
        } else {
            x_head <- head(object, nhead)
            x_rownames <- rownames(x_head)
            if (ntail == 0L) {
                m <- rbind(makeNakedCharacterMatrixForDisplay(x_head),
                           rbind(rep.int("...", x_ncol)))
            } else {
                i <- c(seq_len(nhead), (x_nrow + 1L) - rev(seq_len(ntail)))
                df <- DataFrame(as.data.frame(object[i, , drop = FALSE]))
                m <- rbind(makeNakedCharacterMatrixForDisplay(head(df, nhead)),
                           rbind(rep.int("...", x_ncol)),
                           makeNakedCharacterMatrixForDisplay(tail(df, ntail)))
                x_rownames <- c(x_rownames, rownames(tail(object, ntail)))
            }
            rownames(m) <- S4Vectors:::make_rownames_for_RectangularData_display(x_rownames, x_nrow, nhead, ntail)
        }
        m <- rbind(rep.int("<DuckDBColumn>", ncol(object)), m)
        print(m, quote = FALSE, right = TRUE)
    }

    invisible(NULL)
})

#' @export
setMethod("length", "DuckDBDataFrame", function(x) ncol(x))

#' @export
#' @importFrom BiocGenerics colnames
setMethod("names", "DuckDBDataFrame", function(x) colnames(x))

#' @export
#' @importFrom BiocGenerics rownames<-
setReplaceMethod("rownames", "DuckDBDataFrame", function(x, value) {
    if (.has.row_number(x)) {
        stop("cannot replace row numbers with rownames")
    }
    keydimnames(x) <- list(value)
    x
})

#' @export
#' @importFrom BiocGenerics colnames<-
#' @importFrom S4Vectors mcols mcols<-
setReplaceMethod("names", "DuckDBDataFrame", function(x, value) {
    colnames(x) <- value
    mc <- mcols(x)
    if (!is.null(mc)) {
        rownames(mcols(mc)) <- value
    }
    x
})

#' @export
#' @importFrom S4Vectors extractROWS
#' @importFrom stats setNames
setMethod("extractROWS", "DuckDBDataFrame", function(x, i) {
    if (missing(i)) {
        return(x)
    }
    i <- setNames(list(i), names(x@keycols))
    .subset_DuckDBTable(x, i = i)
})

.head_conn <- function(x, n) {
    conn <- head(x@conn, n)
    keycols <- x@keycols
    keycols[[1L]] <- .keycols.row_number(conn)
    initialize2(x, conn = conn, keycols = keycols, check = FALSE)
}

#' @export
#' @importFrom S4Vectors head isSingleNumber
setMethod("head", "DuckDBDataFrame", function(x, n = 6L, ...) {
    if (!isSingleNumber(n)) {
        stop("'n' must be a single number")
    }
    if (.has.row_number(x)) {
        return(.head_conn(x, n))
    }
    n <- as.integer(n)
    nr <- nrow(x)
    if (n < 0) {
        n <- max(0L, nr + n)
    }
    if (n > nr) {
        x
    } else {
        extractROWS(x, seq_len(n))
    }
})

#' @export
#' @importFrom S4Vectors isSingleNumber tail
setMethod("tail", "DuckDBDataFrame", function(x, n = 6L, ...) {
    if (!isSingleNumber(n)) {
        stop("'n' must be a single number")
    }
    if ((n > 0L) && .has.row_number(x)) {
        stop("tail requires a keycols to be efficient")
    }
    n <- as.integer(n)
    nr <- nrow(x)
    if (n < 0) {
        n <- max(0L, nr + n)
    }
    if (n > nr) {
        x
    } else {
        extractROWS(x, (nr + 1L) - rev(seq_len(n)))
    }
})

#' @export
#' @importFrom stats setNames
#' @importFrom S4Vectors extractCOLS mcols normalizeSingleBracketSubscript
setMethod("extractCOLS", "DuckDBDataFrame", function(x, i) {
    if (missing(i)) {
        return(x)
    }
    xstub <- setNames(seq_along(x), names(x))
    i <- normalizeSingleBracketSubscript(i, xstub)
    if (anyDuplicated(i)) {
        stop("cannot extract duplicate columns in a DuckDBDataFrame")
    }
    mc <- extractROWS(mcols(x), i)
    .subset_DuckDBTable(x, j = i, elementMetadata = mc)
})

#' @export
setMethod("[", "DuckDBDataFrame", function(x, i, j, ..., drop = TRUE) {
    if (!missing(i)) {
        x <- extractROWS(x, i)
    }
    if (!missing(j)) {
        x <- extractCOLS(x, j)
    }
    if (missing(drop)) {
        drop <- (ncol(x) == 1L)
    }
    if (drop && (ncol(x) == 1L)) {
        x <- x[[1L]]
    }
    x
})

#' @exportS3Method base::subset
subset.DuckDBDataFrame <- function(x, subset, select, drop = FALSE, ...) {
    if (!missing(subset)) {
        i <- eval(substitute(subset), as.env(x), parent.frame())
        x <- extractROWS(x, i)
    }
    if (!missing(select)) {
        j <- S4Vectors:::evalqForSelect(select, x, ...)
        x <- extractCOLS(x, j)
    }
    if (missing(drop)) {
        drop <- (ncol(x) == 1L)
    }
    if (drop && (ncol(x) == 1L)) {
        x <- x[[1L]]
    }
    x
}

#' @export
#' @importFrom S4Vectors subset
setMethod("subset", "DuckDBDataFrame", subset.DuckDBDataFrame)

#' @export
#' @importFrom S4Vectors new2 normalizeDoubleBracketSubscript
setMethod("[[", "DuckDBDataFrame", function(x, i, j, ...) {
    if (!missing(j)) {
        stop("list-style indexing of a DuckDBDataFrame with non-missing 'j' is not supported")
    }

    if (missing(i) || length(i) != 1L) {
        stop("expected a length-1 'i' for list-style indexing of a DuckDBDataFrame")
    }

    i <- normalizeDoubleBracketSubscript(i, x)
    column <- extractCOLS(x, i)
    new2("DuckDBColumn", table = as(column, "DuckDBTable"), metadata = as.list(mcols(column)), check = FALSE)
})

#' @export
#' @importFrom S4Vectors replaceROWS
setMethod("replaceROWS", "DuckDBDataFrame", function(x, i, value) {
    stop("replacement of rows in a DuckDBDataFrame is not supported")
})

#' @export
#' @importFrom S4Vectors new2 normalizeSingleBracketReplacementValue
setMethod("normalizeSingleBracketReplacementValue", "DuckDBDataFrame",
function(value, x) {
    if (is(value, "DuckDBColumn")) {
        return(new2("DuckDBDataFrame", value@table, check = FALSE))
    }
    callNextMethod()
})

#' @export
#' @importFrom stats setNames
#' @importFrom S4Vectors replaceCOLS normalizeSingleBracketSubscript
setMethod("replaceCOLS", "DuckDBDataFrame", function(x, i, value) {
    xstub <- setNames(seq_along(x), names(x))
    i2 <- normalizeSingleBracketSubscript(i, xstub, allow.append = TRUE)
    if (is(value, "DuckDBDataFrame")) {
        if (isTRUE(all.equal(x, value))) {
            datacols <- x@datacols
            datacols[i2] <- unname(value@datacols)
            if (is.character(i)) {
                names(datacols)[i2] <- i
            }
            return(initialize2(x, datacols = datacols, check = FALSE))
        }
    }
    stop("not compatible DuckDBDataFrame objects")
})

#' @export
#' @importFrom S4Vectors normalizeDoubleBracketSubscript
setMethod("[[<-", "DuckDBDataFrame", function(x, i, j, ..., value) {
    if (is.character(i)) {
        i2 <- i
    } else {
        i2 <- normalizeDoubleBracketSubscript(i, x, allow.nomatch = TRUE)
    }
    if (length(i2) == 1L && !is.na(i2)) {
        if (is(value, "DuckDBColumn")) {
            if (isTRUE(all.equal(as(x, "DuckDBTable"), value@table))) {
                datacols <- x@datacols
                datacols[[i2]] <- value@table@datacols[[1L]]
                return(initialize2(x, datacols = datacols, check = FALSE))
            }
        }
    }
    stop("not compatible DuckDBDataFrame and DuckDBColumn objects")
})

#' @export
#' @importFrom S4Vectors bindROWS
setMethod("bindROWS", "DuckDBDataFrame",
function(x, objects = list(), use.names = TRUE, ignore.mcols = FALSE, check = TRUE) {
    stop("binding rows to a DuckDBDataFrame is not supported")
})

#' @export
#' @importFrom dplyr rename
#' @importFrom S4Vectors combineRows make_zero_col_DFrame mcols mcols<- metadata new2
cbind.DuckDBDataFrame <- function(..., deparse.level = 1) {
    objects <- list(...)

    all_mcols <- vector("list", length(objects))
    all_metadata <- vector("list", length(objects))
    has_mcols <- FALSE

    for (i in seq_along(objects)) {
        obj <- objects[[i]]

        md <- list()
        mc <- make_zero_col_DFrame(NCOL(obj))
        if (is(obj, "DuckDBColumn")) {
            table <- obj@table
            cname <- names(objects)[i]
            if (!is.null(cname)) {
                colnames(table) <- cname
            }
            objects[[i]] <- new2("DuckDBDataFrame", table, check = FALSE)
            md <- metadata(obj)
            if (length(md) > 0L) {
                has_mcols <- TRUE
                mc <- new2("DFrame", rownames = cname, nrows = 1L, listData = md, check = FALSE)
                md <- list()
            }
        } else if (is(obj, "DuckDBDataFrame")) {
            mc <- mcols(obj, use.names = FALSE) %||% mc
            if (ncol(mc) > 0L) {
                has_mcols <- TRUE
                mcols(objects[[i]]) <- NULL
            }
            md <- metadata(obj)
        }
        all_mcols[[i]] <- mc
        all_metadata[[i]] <- md
    }

    x <- objects[[1L]]
    objects <- objects[-1L]
    bound <- bindCOLS(x, objects)

    if (has_mcols) {
        all_mcols <- do.call(combineRows, all_mcols)
        rownames(all_mcols) <- colnames(bound)
    } else {
        all_mcols <- NULL
    }
    all_metadata <- do.call(c, all_metadata)

    initialize2(bound, elementMetadata = all_mcols, metadata = all_metadata, check = FALSE)
}

#' @export
setMethod("cbind", "DuckDBDataFrame", cbind.DuckDBDataFrame)

#' @export
#' @importFrom S4Vectors as.env
setMethod("as.env", "DuckDBDataFrame",
function(x, enclos = parent.frame(2), tform = identity) {
    S4Vectors:::makeEnvForNames(x, colnames(x), enclos, tform)
})

#' @export
#' @importFrom BiocGenerics as.data.frame
#' @importFrom stats setNames
setMethod("as.data.frame", "DuckDBDataFrame",
function(x, row.names = NULL, optional = FALSE, ...) {
    # as.data.frame,DuckDBTable-method
    df <- callNextMethod(x, row.names = row.names, optional = optional, ...)

    # Add rownames, renaming if specified
    keyname <- names(x@keycols)
    if (is.null(names(x@keycols[[1L]]))) {
        rnames <- df[[keyname]]
    } else {
        rnames <- x@keycols[[1L]]
        rnames <- setNames(names(rnames), rnames)
        rnames <- rnames[as.character(df[[keyname]])]
    }
    rownames(df) <- rnames

    df[rownames(x), colnames(x), drop = FALSE]
})

#' @export
#' @importFrom dplyr everything select
#' @importFrom S4Vectors isSingleString new2
#' @importFrom stats setNames
#' @rdname DuckDBDataFrame
DuckDBDataFrame <- function(conn, keycols, datacols, ...) {
    if (missing(datacols)) {
        tbl <- DuckDBTable(conn, keycols = keycols, ...)
    } else {
        tbl <- DuckDBTable(conn, keycols = keycols, datacols = datacols, ...)
    }
    new2("DuckDBDataFrame", tbl, ..., check = FALSE)
}
