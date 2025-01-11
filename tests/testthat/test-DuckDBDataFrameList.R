# Tests the basic functions of a DuckDBDataFrameList.
# library(testthat); library(BiocDuckDB); source("setup.R"); source("test-DuckDBDataFrameList.R")

test_that("basic methods work as expected for a DuckDBDataFrameList", {
    df <- DuckDBDataFrame(mtcars_parquet, datacols = colnames(mtcars), keycol = list(model = rownames(mtcars)))
    dflist <- split(df, df[["carb"]])

    expected <- DataFrame(mtcars)
    expected <- split(expected, expected[["carb"]])

    checkDuckDBDataFrameList(dflist, expected)
})

test_that("element metadata work as expected for a DuckDBDataFrameList", {
    df <- DuckDBDataFrame(mtcars_parquet, datacols = colnames(mtcars), keycol = list(model = rownames(mtcars)))
    dflist <- split(df, df[["carb"]])
    mcols(dflist) <- as.list(head(letters, length(dflist)))

    expected <- DataFrame(mtcars)
    expected <- split(expected, expected[["carb"]])
    mcols(expected) <- as.list(head(letters, length(expected)))

    checkDuckDBDataFrameList(dflist, expected)
})

test_that("column metadata work as expected for a DuckDBDataFrameList", {
    df <- DuckDBDataFrame(mtcars_parquet, datacols = colnames(mtcars), keycol = list(model = rownames(mtcars)))
    mcols(df) <- head(letters, ncol(df)[1L])
    dflist <- split(df, df[["carb"]])

    expected <- DataFrame(mtcars)
    mcols(expected) <- head(letters, ncol(expected)[1L])
    expected <- split(expected, expected[["carb"]])

    checkDuckDBDataFrameList(dflist, expected)


    df <- DuckDBDataFrame(mtcars_parquet, datacols = colnames(mtcars), keycol = list(model = rownames(mtcars)))
    dflist <- split(df, df[["carb"]])
    columnMetadata(dflist) <- head(letters, ncols(dflist)[1L])

    expected <- DataFrame(mtcars)
    expected <- split(expected, expected[["carb"]])
    columnMetadata(expected) <- head(letters, ncols(expected)[1L])

    checkDuckDBDataFrameList(dflist, expected)
})

test_that("renaming list elements work for a DuckDBDataFrameList", {
    df <- DuckDBDataFrame(mtcars_parquet, datacols = colnames(mtcars), keycol = list(model = rownames(mtcars)))
    dflist <- split(df, df[["carb"]])
    names(dflist) <- head(letters, length(dflist))

    expected <- DataFrame(mtcars)
    expected <- split(expected, expected[["carb"]])
    names(expected) <- head(letters, length(expected))

    checkDuckDBDataFrameList(dflist, expected)
})

test_that("renaming columns work for a DuckDBDataFrameList", {
    df <- DuckDBDataFrame(mtcars_parquet, datacols = colnames(mtcars), keycol = list(model = rownames(mtcars)))
    dflist <- split(df, df[["carb"]])
    colnames(dflist) <- head(letters, ncols(dflist)[1L])

    expected <- DataFrame(mtcars)
    expected <- split(expected, expected[["carb"]])
    colnames(expected) <- head(letters, ncols(expected)[1L])

    checkDuckDBDataFrameList(dflist, expected)

    df <- DuckDBDataFrame(mtcars_parquet, datacols = colnames(mtcars), keycol = list(model = rownames(mtcars)))
    dflist <- split(df, df[["carb"]])
    commonColnames(dflist) <- head(letters, ncols(dflist)[1L])

    expected <- DataFrame(mtcars)
    expected <- split(expected, expected[["carb"]])
    commonColnames(expected) <- head(letters, ncols(expected)[1L])

    checkDuckDBDataFrameList(dflist, expected)
})

test_that("subscripting works for a DuckDBDataFrameList", {
    df <- DuckDBDataFrame(mtcars_parquet, datacols = colnames(mtcars), keycol = list(model = rownames(mtcars)))
    dflist <- split(df, df[["carb"]])

    expected <- DataFrame(mtcars)
    expected <- split(expected, expected[["carb"]])

    checkDuckDBDataFrameList(dflist[c(5, 3)], expected[c(5, 3)])

    checkDuckDBDataFrame(dflist[[4]], as.data.frame(expected[[4]]))
})

test_that("head works for a DuckDBDataFrameList", {
    df <- DuckDBDataFrame(mtcars_parquet, datacols = colnames(mtcars), keycol = list(model = rownames(mtcars)))
    dflist <- split(df, df[["carb"]])

    expected <- DataFrame(mtcars)
    expected <- split(expected, expected[["carb"]])

    checkDuckDBDataFrameList(head(dflist, 0), head(expected, 0))
    checkDuckDBDataFrameList(head(dflist, 3), head(expected, 3))
})

test_that("tail works for a DuckDBDataFrameList", {
    df <- DuckDBDataFrame(mtcars_parquet, datacols = colnames(mtcars), keycol = list(model = rownames(mtcars)))
    dflist <- split(df, df[["carb"]])

    expected <- DataFrame(mtcars)
    expected <- split(expected, expected[["carb"]])

    checkDuckDBDataFrameList(tail(dflist, 0), tail(expected, 0))
    checkDuckDBDataFrameList(tail(dflist, 3), tail(expected, 3))
})

test_that("coersion to a DFrameList works for a DuckDBDataFrameList", {
    df <- DuckDBDataFrame(mtcars_parquet, datacols = colnames(mtcars), keycol = list(model = rownames(mtcars)))
    dflist <- as(split(df, df[["carb"]]), "DFrameList")

    expected <- DataFrame(mtcars)
    expected <- split(expected, expected[["carb"]])

    for (i in names(dflist)) {
        object_i <- dflist[[i]]
        expected_i <- expected[[i]]
        expected_i <- expected_i[rownames(object_i), , drop = FALSE]
        expect_identical(object_i, expected_i)
    }
})
