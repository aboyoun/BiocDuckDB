#' DuckDB-backed Genomic Ranges
#'
#' @description
#' Create a DuckDB-backed \linkS4class{GenomicRanges} object.
#'
#' @param conn Either a character vector containing the paths to parquet, csv,
#' or gzipped csv data files; a string that defines a duckdb \code{read_*} data
#' source; a \code{DuckDBDataFrame} object; or a \code{tbl_duckdb_connection}
#' object.
#' @param seqnames Either \code{NULL} or a string specifying the column from
#' \code{conn} that defines the sequence names.
#' @param start Either \code{NULL} or a string specifying the column from
#' \code{conn} that defines the start of the range.
#' @param end Either \code{NULL} or a string specifying the column from
#' \code{conn} that defines the end of the range.
#' @param width Either \code{NULL} or a string specifying the column from
#' \code{conn} that defines the width of the range.
#' @param strand Either \code{NULL} or a string specifying the column from
#' \code{conn} that defines the width of the range.
#' @param keycols An optional character vector of column names from \code{conn}
#' that will define the primary key, or a named list of character vectors where
#' the names of the list define the key and the character vectors set the
#' distinct values for the key. If missing, a \code{row_number} column is
#' created as an identifier.
#' @param mcols Optional character vector specifying the columns that define
#' the metadata columns.
#' @param seqinfo Either \code{NULL}, or a \code{\link{Seqinfo}} object, or a
#' character vector of unique sequence names (a.k.a. seqlevels), or a named
#' numeric vector of sequence lengths.
#' @param seqlengths Either \code{NULL}, or an integer vector named with
#' \code{levels(seqnames)} and containing the \code{lengths} (or \code{NA}) for
#' each level in \code{levels(seqnames)}.
#'
#' @author Patrick Aboyoun
#'
#' @examples
#' # Create an example data set with start and width columns:
#' df <- data.frame(id = head(letters, 10),
#'                  seqnames = rep.int(c("chr2", "chr2", "chr1", "chr3"), c(1, 3, 2, 4)),
#'                  start = 1:10, width = 10:1,
#'                  strand = strand(rep.int(c("-", "+", "*", "+", "-"), c(1, 2, 2, 3, 2))),
#'                  score = 1:10,
#'                  GC = seq(1, 0, length = 10))
#' tf <- tempfile(fileext = ".parquet")
#' on.exit(unlink(tf))
#' arrow::write_parquet(df, tf)
#'
#' # Create the DuckDBGRanges object
#' seqinfo <- GenomeInfoDb::Seqinfo(paste0("chr", 1:3), c(1000, 2000, 1500), NA, "mock1")
#' gr <- DuckDBGRanges(tf, seqnames = "seqnames", start = "start", width = "width",
#'                     strand = "strand", mcols = c("score", "GC"), seqinfo = seqinfo,
#'                     keycols = "id")
#' gr
#'
#' @aliases
#' DuckDBGRanges-class
#' show,DuckDBGRanges-method
#' [,DuckDBGRanges,ANY,ANY,ANY-method
#' as.data.frame,DuckDBGRanges-method
#' coerce,DuckDBGRanges,DuckDBDataFrame-method
#' dbconn,DuckDBGRanges-method
#' elementMetadata,DuckDBGRanges-method
#' elementMetadata<-,DuckDBGRanges-method
#' end,DuckDBGRanges-method
#' extractROWS,DuckDBGRanges,ANY-method
#' head,DuckDBGRanges-method
#' length,DuckDBGRanges-method
#' names,DuckDBGRanges-method
#' ranges,DuckDBGRanges-method
#' seqinfo,DuckDBGRanges-method
#' seqnames,DuckDBGRanges-method
#' start,DuckDBGRanges-method
#' strand,DuckDBGRanges-method
#' tail,DuckDBGRanges-method
#' tblconn,DuckDBGRanges-method
#' width,DuckDBGRanges-method
#'
#' @include DuckDBDataFrame.R
#'
#' @name DuckDBGRanges
NULL

.datacols_granges <- expression(seqnames = NULL, start = NULL, end = NULL, width = NULL, strand = NULL)

#' @export
#' @importClassesFrom GenomicRanges GenomicRanges
#' @importClassesFrom GenomeInfoDb Seqinfo
setClass("DuckDBGRanges", contains = "GenomicRanges",
         slots = c(frame = "DuckDBDataFrame", seqinfo = "Seqinfo"),
         prototype = prototype(frame = new2("DuckDBDataFrame", datacols = .datacols_granges, check = FALSE)))

#' @importFrom S4Vectors mcols
setValidity2("DuckDBGRanges", function(x) {
    msg <- NULL
    frame <- x@frame
    if (!setequal(colnames(frame), names(.datacols_granges))) {
        msg <- c(msg, "missing definition for one or more of 'seqnames', 'start', 'end', 'width', and 'strand'")
    }
    mcols <- x@elementMetadata
    if (!is.null(mcols) && !is(mcols, "DuckDBDataFrame") && !isTRUE(all.equal(mcols, frame))) {
        msg <- c(msg, "'mcols' must be compatible with its associated ranges")
    }
    msg %||% TRUE
})

#' @export
setMethod("show", "DuckDBGRanges", function(object) {
    cat(summary(object), ":\n", sep = "")
    if (length(object) > 0L) {
        m <- .makePrettyCharacterMatrixForDisplay(as(object, "DuckDBDataFrame"))
        k <- NCOL(object@elementMetadata)
        if (k > 0L) {
            nc <- ncol(m)
            h <- nc - k
            m <- cbind(m[, 1:h, drop = FALSE],
                       `|` = ifelse(rownames(m) == "...", ".", "|"),
                       m[, (h + 1L):nc, drop = FALSE])
        }
        print(m, quote = FALSE, right = TRUE)
    }
    cat("-------\n")
    cat("seqinfo: ", summary(object@seqinfo), "\n", sep = "")
})

#' @export
#' @importFrom BiocGenerics dbconn
setMethod("dbconn", "DuckDBGRanges", function(x) callGeneric(x@frame))

#' @export
setMethod("tblconn", "DuckDBGRanges", function(x) callGeneric(x@frame))

setMethod(".keycols", "DuckDBGRanges", function(x) callGeneric(x@frame))

setMethod(".has_row_number", "DuckDBGRanges", function(x) callGeneric(x@frame))

#' @export
setMethod("length", "DuckDBGRanges", function(x) nrow(x@frame))

#' @export
setMethod("names", "DuckDBGRanges", function(x) rownames(x@frame))

#' @export
#' @importFrom GenomeInfoDb seqinfo
setMethod("seqinfo", "DuckDBGRanges", function(x) x@seqinfo)

#' @export
#' @importFrom GenomeInfoDb seqnames
setMethod("seqnames", "DuckDBGRanges", function(x) x@frame[["seqnames"]])

#' @export
#' @importFrom BiocGenerics start
setMethod("start", "DuckDBGRanges", function(x, ...) x@frame[["start"]])

#' @export
#' @importFrom BiocGenerics end
setMethod("end", "DuckDBGRanges", function(x, ...) x@frame[["end"]])

#' @export
#' @importFrom BiocGenerics width
setMethod("width", "DuckDBGRanges", function(x) x@frame[["width"]])

#' @export
#' @importFrom BiocGenerics strand
setMethod("strand", "DuckDBGRanges", function(x, ...) x@frame[["strand"]])

#' @export
#' @importFrom IRanges ranges
setMethod("ranges", "DuckDBGRanges", function (x, use.names = TRUE, use.mcols = FALSE, ...) {
    df <- x@frame[, c("start", "end", "width"), drop = FALSE]
    if (use.mcols && !is.null(x@elementMetadata)) {
        df <- cbind.DuckDBDataFrame(df, x@elementMetadata)
    }
    df
})

#' @export
#' @importFrom S4Vectors elementMetadata
setMethod("elementMetadata", "DuckDBGRanges", function(x) x@elementMetadata)

#' @export
#' @importFrom S4Vectors elementMetadata<-
setReplaceMethod("elementMetadata", "DuckDBGRanges", function(x, ..., value) {
    if (!is(value, "DuckDBDataFrame") || !isTRUE(all.equal(x@frame, value))) {
        stop("'elementMetadata' must be a compatible DuckDBDataFrame object")
    }
    replaceSlots(x, elementMetadata = value, check = FALSE)
})

#' @export
#' @importFrom S4Vectors extractROWS
setMethod("extractROWS", "DuckDBGRanges", function(x, i) {
    if (missing(i)) {
        return(x)
    }

    frame <- x@frame
    frame <- callGeneric(frame, i = i)

    mcols <- x@elementMetadata
    if (!is.null(mcols)) {
        mcols <- callGeneric(mcols, i = i)
    }

    replaceSlots(x, frame = frame, elementMetadata = mcols, check = FALSE)
})

#' @export
#' @importFrom S4Vectors head
setMethod("head", "DuckDBGRanges", function(x, n = 6L, ...) {
    frame <- x@frame
    frame <- callGeneric(frame, n = n, ...)

    mcols <- x@elementMetadata
    if (!is.null(mcols)) {
        mcols <- callGeneric(mcols, n = n, ...)
    }

    replaceSlots(x, frame = frame, elementMetadata = mcols, check = FALSE)
})

#' @export
#' @importFrom S4Vectors tail
setMethod("tail", "DuckDBGRanges", function(x, n = 6L, ...) {
    frame <- x@frame
    frame <- callGeneric(frame, n = n, ...)

    mcols <- x@elementMetadata
    if (!is.null(mcols)) {
        mcols <- callGeneric(mcols, n = n, ...)
    }

    replaceSlots(x, frame = frame, elementMetadata = mcols, check = FALSE)
})

#' @export
setMethod("[", "DuckDBGRanges", function(x, i, j, ..., drop = TRUE) {
    frame <- x@frame
    if (!missing(i)) {
        frame <- extractROWS(frame, i)
    }

    mcols <- x@elementMetadata
    if (!is.null(mcols)) {
        if (!missing(i)) {
            mcols <- extractROWS(mcols, i)
        }
        if (!missing(j)) {
            mcols <- extractCOLS(mcols, j)
        }
    }

    replaceSlots(x, frame = frame, elementMetadata = mcols, check = FALSE)
})

#' @export
setAs("DuckDBGRanges", "DuckDBDataFrame", function(from) {
    df <- from@frame
    if (!is.null(from@elementMetadata)) {
        df <- cbind.DuckDBDataFrame(df, from@elementMetadata)
    }
    df
})

#' @export
#' @importFrom BiocGenerics as.data.frame
setMethod("as.data.frame", "DuckDBGRanges",
function(x, row.names = NULL, optional = FALSE, ...) {
    df <- as(x, "DuckDBDataFrame")
    callGeneric(df, row.names = row.names, optional = optional, ...)
})

#' @export
#' @importFrom dplyr distinct pull select
#' @importFrom GenomeInfoDb Seqinfo
#' @importFrom S4Vectors isSingleString new2
#' @importFrom stats setNames
#' @rdname DuckDBGRanges
DuckDBGRanges <-
function(conn, seqnames, start = NULL, end = NULL, width = NULL, strand = NULL,
         keycols = NULL, mcols = NULL, seqinfo = NULL, seqlengths = NULL)
{
    datacols <- .datacols_granges

    stringAsName <- function(x) if (isSingleString(x)) as.name(x) else x

    nargs_range <- sum(c(!is.null(start), !is.null(end), !is.null(width)))
    if ((nargs_range == 1L) && !is.null(start)) {
        end <- start
        width <- 1L
    } else if (nargs_range != 2L) {
        stop("must provide exactly two of 'start', 'end', and 'width'")
    }

    datacols[["seqnames"]] <- stringAsName(seqnames) %||% ""
    datacols[["start"]] <- stringAsName(start)
    datacols[["end"]] <- stringAsName(end)
    datacols[["width"]] <- stringAsName(width)
    datacols[["strand"]] <- stringAsName(strand) %||% "*"

    if (is.null(datacols[["start"]])) {
        datacols[["start"]] <- call("+", call("-", datacols[["end"]], datacols[["width"]]), 1L)
    } else if (is.null(datacols[["end"]])) {
        datacols[["end"]] <- call("-", call("+", datacols[["start"]], datacols[["width"]]), 1L)
    } else if (is.null(datacols[["width"]])) {
        datacols[["width"]] <- call("+", call("-", datacols[["end"]], datacols[["start"]]), 1L)
    }

    datacols <- datacols[c("seqnames", "start", "end", "width", "strand")]
    ccols <- datacols
    if (length(mcols) == 0L) {
        mcols <- NULL
    } else {
        if (is.character(mcols)) {
            mcols <- sapply(mcols, as.name, simplify = FALSE)
        }
        mcols <- as.expression(mcols)
        ccols <- c(ccols, mcols)
    }

    comb <- DuckDBDataFrame(conn, datacols = ccols, keycols = keycols)
    frame <- comb[, names(datacols), drop = FALSE]
    if (is.null(mcols)) {
        mcols <- comb[, character(), drop = FALSE]
    } else {
        mcols <- comb[, names(mcols), drop = FALSE]
    }

    seqinfo <- GenomicRanges:::normarg_seqinfo2(seqinfo, seqlengths)
    if (is.null(seqinfo)) {
        seqinfo <- Seqinfo(sort(pull(distinct(select(frame@conn, !!!as.list(seqnames))))))
    } else {
        seqinfo <- GenomicRanges:::normarg_seqinfo1(seqinfo)
    }

    new2("DuckDBGRanges", frame = frame, seqinfo = seqinfo, elementMetadata = mcols, check = FALSE)
}
