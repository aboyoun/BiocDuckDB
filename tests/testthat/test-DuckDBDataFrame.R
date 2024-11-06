# Tests the basic functions of a DuckDBDataFrame.
# library(testthat); library(BiocDuckDB); source("setup.R"); source("test-DuckDBDataFrame.R")

test_that("basic methods work for a DuckDBDataFrame", {
    df <- DuckDBDataFrame(mtcars_path, keycols = "model")
    checkDuckDBDataFrame(df, mtcars)

    df <- DuckDBDataFrame(mtcars_path, keycols = list(model = rownames(mtcars)))
    checkDuckDBDataFrame(df, mtcars)
    expect_identical(rownames(df), rownames(mtcars))
    expect_identical(as.data.frame(df), mtcars)

    df <- DuckDBDataFrame(infert_path)
    checkDuckDBDataFrame(df, infert)

    df <- DuckDBDataFrame(infert_path, datacols = colnames(infert))
    checkDuckDBDataFrame(df, infert)
})

test_that("renaming columns creates a new DuckDBDataFrame", {
    df <- DuckDBDataFrame(mtcars_path, keycols = list(model = rownames(mtcars)))
    expected <- mtcars

    replacements <- sprintf("COL%d", seq_len(ncol(df)))
    colnames(df) <- replacements
    colnames(expected) <- replacements
    checkDuckDBDataFrame(df, expected)
})

test_that("adding rownames creates a new DuckDBDataFrame", {
    df <- DuckDBDataFrame(mtcars_path, keycols = "model")
    expected <- mtcars

    replacements <- sprintf("ROW%d", seq_len(nrow(df)))
    rownames(df) <- replacements
    rownames(expected) <- setNames(names(df@keycols[[1L]]), df@keycols[[1L]])[rownames(expected)]
    checkDuckDBDataFrame(df, expected)

    df <- DuckDBDataFrame(mtcars_path, keycols = list(model = rownames(mtcars)))
    expected <- mtcars

    replacements <- sprintf("ROW%d", seq_len(nrow(df)))
    rownames(df) <- replacements
    rownames(expected) <- replacements
    checkDuckDBDataFrame(df, expected)
})

test_that("slicing by columns preserves type of a DuckDBDataFrame", {
    df <- DuckDBDataFrame(mtcars_path, keycols = list(model = rownames(mtcars)))

    keep <- 1:2
    checkDuckDBDataFrame(df[,keep], mtcars[,keep])

    keep <- colnames(df)[c(4,2,3)]
    checkDuckDBDataFrame(df[,keep], mtcars[,keep])

    keep <- startsWith(colnames(df), "d")
    checkDuckDBDataFrame(df[,keep], mtcars[,keep])

    keep <- 5
    checkDuckDBDataFrame(df[,keep, drop=FALSE], mtcars[,keep, drop=FALSE])

    # Respects mcols.
    copy <- df
    mcols(copy) <- DataFrame(whee=seq_len(ncol(df)))
    copy <- copy[,3:1]
    expect_identical(mcols(copy)$whee, 3:1)

    # Respects metadata.
    copy <- df
    mcols(copy) <- mtcars_mcols
    expect_identical(metadata(copy[["carb"]]),
                     as.list(mtcars_mcols["carb", "description", drop=FALSE]))

    # Respects metadata when extracting columns.
    copy <- df
    mcols(copy) <- mtcars_mcols
    copy <- cbind(copy[, c(2, 4, 6)], copy[[1]])
    expect_identical(mcols(copy), mtcars_mcols[c(2, 4, 6, 1), , drop = FALSE])
})

test_that("extraction of a column yields a DuckDBColumn", {
    df <- DuckDBDataFrame(mtcars_path, keycols = list(model = rownames(mtcars)))

    keep <- 5
    checkDuckDBColumn(df[,keep], setNames(mtcars[,keep], rownames(mtcars)))

    keep <- colnames(df)[5]
    checkDuckDBColumn(df[,keep], setNames(mtcars[,keep], rownames(mtcars)))
})

test_that("conditional slicing by rows preserves type of a DuckDBDataFrame", {
    df <- DuckDBDataFrame(mtcars_path, keycols = list(model = rownames(mtcars)))
    checkDuckDBDataFrame(df[df$cyl > 6,], mtcars[mtcars$cyl > 6,])
})

test_that("positional slicing by rows preserves type of a DuckDBDataFrame", {
    df <- DuckDBDataFrame(mtcars_path, keycols = list(model = rownames(mtcars)))
    i <- sample(nrow(df))
    checkDuckDBDataFrame(df[i,], mtcars[i,])
})

test_that("head preserves type of a DuckDBDataFrame", {
    df <- DuckDBDataFrame(mtcars_path, keycols = list(model = rownames(mtcars)))
    checkDuckDBDataFrame(head(df, 0), head(mtcars, 0))

    df <- DuckDBDataFrame(mtcars_path, keycols = list(model = rownames(mtcars)))
    checkDuckDBDataFrame(head(df, 20), head(mtcars, 20))
})

test_that("tail preserves type of a DuckDBDataFrame", {
    df <- DuckDBDataFrame(mtcars_path, keycols = list(model = rownames(mtcars)))
    checkDuckDBDataFrame(tail(df, 0), tail(mtcars, 0))

    df <- DuckDBDataFrame(mtcars_path, keycols = list(model = rownames(mtcars)))
    checkDuckDBDataFrame(tail(df, 20), tail(mtcars, 20))
})

test_that("subset assignments that produce errors", {
    df <- DuckDBDataFrame(mtcars_path, keycols = list(model = rownames(mtcars)))
    expect_error(df[1:5,] <- df[9:13,])
    expect_error(df[,"foobar"] <- runif(nrow(df)))
    expect_error(df$some_random_thing <- runif(nrow(df)))
})

test_that("subset assignments that return a DuckDBDataFrame", {
    df <- DuckDBDataFrame(mtcars_path, keycols = list(model = rownames(mtcars)))

    copy <- df
    copy[,1] <- copy[,1]
    checkDuckDBDataFrame(copy, mtcars)

    copy <- df
    copy[,colnames(df)[2]] <- copy[,colnames(df)[2],drop=FALSE]
    checkDuckDBDataFrame(copy, mtcars)

    copy <- df
    copy[[3]] <- copy[[3]]
    checkDuckDBDataFrame(copy, mtcars)

    copy <- df
    copy[[1]] <- copy[[3]]
    mtcars2 <- mtcars
    mtcars2[[1]] <- mtcars2[[3]]
    checkDuckDBDataFrame(copy, mtcars2)

    copy <- df
    copy[,c(1,2,3)] <- copy[,c(4,5,6)]
    mtcars2 <- mtcars
    mtcars2[,c(1,2,3)] <- mtcars2[,c(4,5,6)]
    checkDuckDBDataFrame(copy, mtcars2)
})

test_that("rbinding produces errors", {
    df <- DuckDBDataFrame(mtcars_path, keycols = list(model = rownames(mtcars)))
    expect_error(rbind(df, df))
})

test_that("cbinding operations that return a DuckDBDataFrame", {
    df <- DuckDBDataFrame(mtcars_path, keycols = list(model = rownames(mtcars)))

    # Same path, we get another PDF.
    checkDuckDBDataFrame(cbind(df, foo=df[["carb"]]), cbind(mtcars, foo=mtcars[["carb"]]))

    # Duplicate names causes unique renaming.
    expected <- cbind(mtcars, mtcars)
    colnames(expected) <- make.unique(colnames(expected), sep="_")
    checkDuckDBDataFrame(cbind(df, df), expected)

    # Duplicate names causes unique renaming.
    expected <- cbind(mtcars, carb=mtcars[["carb"]])
    colnames(expected) <- make.unique(colnames(expected), sep="_")
    checkDuckDBDataFrame(cbind(df, carb=df[["carb"]]), expected)

    # Duplicate names causes unique renaming.
    expected <- cbind(carb=mtcars[["carb"]], mtcars)
    colnames(expected) <- make.unique(colnames(expected), sep="_")
    checkDuckDBDataFrame(cbind(carb=df[["carb"]], df), expected)
})

test_that("cbinding operations that produce errors", {
    df <- DuckDBDataFrame(mtcars_path, keycols = list(model = rownames(mtcars)))

    expect_error(cbind(df, mtcars))

    # Different paths causes an error.
    tmp <- tempfile()
    file.symlink(mtcars_path, tmp)
    df2 <- DuckDBDataFrame(tmp, keycols = list(model = rownames(mtcars)))
    expect_error(cbind(df, df2))
    expect_error(cbind(df, carb=df2[["carb"]]))
})

test_that("cbinding carries forward any metadata", {
    df <- DuckDBDataFrame(mtcars_path, keycols = list(model = rownames(mtcars)))

    df1 <- df
    colnames(df1) <- paste0(colnames(df1), "_1")
    mcols(df1) <- DataFrame(whee="A")

    df2 <- df
    colnames(df2) <- paste0(colnames(df2), "_2")
    mcols(df2) <- DataFrame(whee="B")

    copy <- cbind(df1, df2)
    expect_s4_class(copy, "DuckDBDataFrame")
    expect_identical(mcols(copy)$whee, rep(c("A", "B"), each=ncol(df)))

    mcols(df1) <- NULL
    copy <- cbind(df1, df2)
    expect_s4_class(copy, "DuckDBDataFrame")
    expect_identical(mcols(copy)$whee, rep(c(NA, "B"), each=ncol(df)))

    metadata(df1) <- list(a="YAY")
    metadata(df2) <- list(a="whee")
    copy <- cbind(df1, df2)
    expect_s4_class(copy, "DuckDBDataFrame")
    expect_identical(metadata(copy), list(a="YAY", a="whee"))
})

test_that("extracting duplicate columns produces errors", {
    df <- DuckDBDataFrame(mtcars_path, keycols = list(model = rownames(mtcars)))
    expect_error(df[,c(1,1,2,2,3,4,3,5)])
})
