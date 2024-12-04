# Tests the basic functions of a DuckDBGRangesList.
# library(testthat); library(BiocDuckDB); source("setup.R"); source("test-DuckDBGRangesList.R")

library(GenomicRanges)

test_that("basic methods work as expected for a DuckDBGRangesList", {
    object <- DuckDBGRanges(granges_tf, seqnames = "seqnames", start = "start", end = "end", strand = "strand", mcols = c("score", "GC", "group"), keycols = "id")
    object <- split(object, object$group)

    expected <- GRanges(granges_df[["seqnames"]], ranges = IRanges(start = granges_df[["start"]], end = granges_df[["end"]], names = granges_df[["id"]]),
                        strand = granges_df[["strand"]], score = granges_df[["score"]], GC = granges_df[["GC"]], group = granges_df[["group"]])
    expected <- split(expected, expected$group)

    checkDuckDBGRangesList(object, expected)
})

test_that("element metadata work as expected for a DuckDBGRangesList", {
    object <- DuckDBGRanges(granges_tf, seqnames = "seqnames", start = "start", end = "end", strand = "strand", mcols = c("score", "GC", "group"), keycols = "id")
    object <- split(object, object$group)
    mcols(object) <- as.list(head(letters, length(object)))

    expected <- GRanges(granges_df[["seqnames"]], ranges = IRanges(start = granges_df[["start"]], end = granges_df[["end"]], names = granges_df[["id"]]),
                        strand = granges_df[["strand"]], score = granges_df[["score"]], GC = granges_df[["GC"]], group = granges_df[["group"]])
    expected <- split(expected, expected$group)
    mcols(expected) <- as.list(head(letters, length(expected)))
    colnames(mcols(expected)) <- colnames(mcols(object))

    checkDuckDBGRangesList(object, expected)
})

test_that("renaming list elements work for a DuckDBGRangesList", {
    object <- DuckDBGRanges(granges_tf, seqnames = "seqnames", start = "start", end = "end", strand = "strand", mcols = c("score", "GC", "group"), keycols = "id")
    object <- split(object, object$group)
    names(object) <- head(letters, length(object))

    expected <- GRanges(granges_df[["seqnames"]], ranges = IRanges(start = granges_df[["start"]], end = granges_df[["end"]], names = granges_df[["id"]]),
                        strand = granges_df[["strand"]], score = granges_df[["score"]], GC = granges_df[["GC"]], group = granges_df[["group"]])
    expected <- split(expected, expected$group)
    names(expected) <- head(letters, length(expected))

    checkDuckDBGRangesList(object, expected)
})

test_that("subscripting works for a DuckDBGRangesList", {
    object <- DuckDBGRanges(granges_tf, seqnames = "seqnames", start = "start", end = "end", strand = "strand", mcols = c("score", "GC", "group"), keycols = "id")
    object <- split(object, object$group)

    expected <- GRanges(granges_df[["seqnames"]], ranges = IRanges(start = granges_df[["start"]], end = granges_df[["end"]], names = granges_df[["id"]]),
                        strand = granges_df[["strand"]], score = granges_df[["score"]], GC = granges_df[["GC"]], group = granges_df[["group"]])
    expected <- split(expected, expected$group)

    checkDuckDBGRangesList(object[c(3, 1)], expected[c(3, 1)])

    checkDuckDBGRanges(object[[2]], expected[[2]])
})

test_that("head works for a DuckDBGRangesList", {
    object <- DuckDBGRanges(granges_tf, seqnames = "seqnames", start = "start", end = "end", strand = "strand", mcols = c("score", "GC", "group"), keycols = "id")
    object <- split(object, object$group)

    expected <- GRanges(granges_df[["seqnames"]], ranges = IRanges(start = granges_df[["start"]], end = granges_df[["end"]], names = granges_df[["id"]]),
                        strand = granges_df[["strand"]], score = granges_df[["score"]], GC = granges_df[["GC"]], group = granges_df[["group"]])
    expected <- split(expected, expected$group)

    checkDuckDBGRangesList(head(object, 0), head(expected, 0))
    checkDuckDBGRangesList(head(object, 2), head(expected, 2))
})

test_that("tail works for a DuckDBGRangesList", {
    object <- DuckDBGRanges(granges_tf, seqnames = "seqnames", start = "start", end = "end", strand = "strand", mcols = c("score", "GC", "group"), keycols = "id")
    object <- split(object, object$group)

    expected <- GRanges(granges_df[["seqnames"]], ranges = IRanges(start = granges_df[["start"]], end = granges_df[["end"]], names = granges_df[["id"]]),
                        strand = granges_df[["strand"]], score = granges_df[["score"]], GC = granges_df[["GC"]], group = granges_df[["group"]])
    expected <- split(expected, expected$group)

    checkDuckDBGRangesList(tail(object, 0), tail(expected, 0))
    checkDuckDBGRangesList(tail(object, 2), tail(expected, 2))
})
