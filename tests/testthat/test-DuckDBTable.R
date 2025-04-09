# Tests the basic functions of a DuckDBTable.
# library(testthat); library(BiocDuckDB); source("setup.R"); source("test-DuckDBTable.R")

test_that("basic methods work as expected for a DuckDBTable", {
    # esoph dataset
    tbl <- DuckDBTable(esoph_parquet, datacols = c("ncases", "ncontrols"), keycols = c("agegp", "alcgp", "tobgp"))
    checkDuckDBTable(tbl, esoph_df)

    tbl <- DuckDBTable(esoph_parquet, datacols = c("ncases", "ncontrols"), keycols = list("agegp" = NULL, "alcgp" = levels(esoph[["alcgp"]]), "tobgp" = NULL))
    checkDuckDBTable(tbl, esoph_df)

    tbl <- DuckDBTable(esoph_parquet, datacols = c("ncases", "ncontrols"),
                            keycols = list("agegp" = levels(esoph[["agegp"]]), "alcgp" = levels(esoph[["alcgp"]]), "tobgp" = levels(esoph[["tobgp"]])))
    checkDuckDBTable(tbl, esoph_df)

    # infert dataset
    tbl <- DuckDBTable(infert_parquet)
    checkDuckDBTable(tbl, infert)

    tbl <- DuckDBTable(infert_parquet, datacols = colnames(infert))
    checkDuckDBTable(tbl, infert)

    # mtcars dataset
    tbl <- DuckDBTable(mtcars_parquet, datacols = colnames(mtcars), keycols = "model")
    checkDuckDBTable(tbl, mtcars_df)

    tbl <- DuckDBTable(mtcars_parquet, datacols = head(colnames(mtcars)), keycols = "model")
    checkDuckDBTable(tbl, mtcars_df[, 1:7])

    # state dataset
    tbl <- DuckDBTable(state_path, datacols = "value", keycols = c("region", "division", "dim1", "dim2"), dimtbls = state_tables)
    checkDuckDBTable(tbl, state_df)

    # titanic dataset
    tbl <- DuckDBTable(titanic_parquet, datacols = "fate", keycols = c("Class", "Sex", "Age", "Survived"))
    checkDuckDBTable(tbl, titanic_df)
})

test_that("keycols names can be modified for a DuckDBTable", {
    tbl <- DuckDBTable(esoph_parquet, datacols = c("ncases", "ncontrols"),
                       keycols = list("agegp" = levels(esoph[["agegp"]]), "alcgp" = levels(esoph[["alcgp"]]), "tobgp" = levels(esoph[["tobgp"]])))
    expect_identical(nkey(tbl), 3L)
    expect_identical(keynames(tbl), c("agegp", "alcgp", "tobgp"))
    expect_identical(keydimnames(tbl), lapply(esoph[, c("agegp", "alcgp", "tobgp")], levels))

    copy <- tbl
    replacement <- vector("list", 3L)
    for (i in seq_along(replacement)) {
        replacement[[i]] <- head(letters, nlevels(esoph[[i]]))
    }
    keydimnames(copy) <- replacement
    expect_identical(keydimnames(copy), setNames(replacement, c("agegp", "alcgp", "tobgp")))

    copy <- tbl
    names(replacement) <- c("agegp", "alcgp", "tobgp")
    keydimnames(copy) <- replacement
    expect_identical(keydimnames(copy), replacement)

    copy <- tbl
    replacement <- list(agegp = levels(esoph$agegp),
                        alcgp = head(LETTERS, nlevels(esoph$alcgp)),
                        tobgp = head(levels(esoph$tobgp)))
    keydimnames(copy) <- replacement["alcgp"]
    expect_identical(keydimnames(copy), replacement)
})

test_that("datacols columns of a DuckDBTable can be cast to a different type", {
    tbl <- DuckDBTable(esoph_parquet, datacols = c("ncases", "ncontrols"), keycols = c("agegp", "alcgp", "tobgp"))
    checkDuckDBTable(tbl, esoph_df)
    expect_identical(coltypes(tbl), c("ncases" = "double", "ncontrols" = "double"))

    coltypes(tbl) <- c("ncases" = "integer", "ncontrols" = "integer")
    checkDuckDBTable(tbl, esoph_df)
    expect_identical(coltypes(tbl), c("ncases" = "integer", "ncontrols" = "integer"))

    tbl <- DuckDBTable(esoph_parquet, datacols = c("ncases", "ncontrols"),
                       keycols = c("agegp", "alcgp", "tobgp"),
                       type = c("ncases" = "integer", "ncontrols" = "integer"))
    checkDuckDBTable(tbl, esoph_df)
    expect_is(as.data.frame(tbl)[["ncases"]], "integer")
    expect_is(as.data.frame(tbl)[["ncontrols"]], "integer")
})

test_that("DuckDBTable column names can be modified", {
    tbl <- DuckDBTable(mtcars_parquet, datacols = colnames(mtcars), keycols = "model")
    replacements <- sprintf("COL%d", seq_len(ncol(tbl)))
    colnames(tbl) <- replacements
    expected <- mtcars_df 
    colnames(expected)[-1L] <- replacements
    checkDuckDBTable(tbl, expected)
})

test_that("nonzero functions work for DuckDBTable", {
    tbl <- DuckDBTable(esoph_parquet, datacols = c("ncases", "ncontrols"), keycols = c("agegp", "alcgp", "tobgp"))
    expected <- cbind(esoph_df[1:3], data.frame(lapply(esoph_df[4:5], is_nonzero)))
    checkDuckDBTable(is_nonzero(tbl), expected)
    expect_equal(nzcount(tbl), sum(expected[, 4:5]))
})

test_that("DuckDBTable can be bound across columns", {
    tbl <- DuckDBTable(mtcars_parquet, datacols = colnames(mtcars), keycols = "model")

    # Same path, we get another PDF.
    checkDuckDBTable(cbind(tbl, foo=tbl[,"carb"]), cbind(mtcars_df, foo=mtcars[["carb"]]))

    # Duplicate names causes unique renaming.
    expected <- cbind(mtcars_df, mtcars)
    colnames(expected) <- make.unique(colnames(expected), sep="_")
    checkDuckDBTable(cbind(tbl, tbl), expected)

    # Duplicate names causes unique renaming.
    expected <- cbind(mtcars_df, carb=mtcars[,"carb"])
    colnames(expected) <- make.unique(colnames(expected), sep="_")
    checkDuckDBTable(cbind(tbl, carb=tbl[,"carb"]), expected)
})

test_that("Arith methods work as expected for a DuckDBTable", {
    tbl <- DuckDBTable(mtcars_parquet, datacols = colnames(mtcars), keycols = "model")

    checkDuckDBTable(tbl + sqrt(tbl), cbind(model = rownames(mtcars), mtcars + sqrt(mtcars)))
    checkDuckDBTable(tbl - tbl[,"carb"], cbind(model = rownames(mtcars), mtcars - mtcars[, "carb"]))
    checkDuckDBTable(tbl * 1L, cbind(model = rownames(mtcars), mtcars * 1L))
    checkDuckDBTable(tbl / 3.14, cbind(model = rownames(mtcars), mtcars / 3.14))
    checkDuckDBTable(sqrt(tbl) ^ tbl, cbind(model = rownames(mtcars), sqrt(mtcars) ^ mtcars))
    checkDuckDBTable(tbl[,"carb"] %% tbl, cbind(model = rownames(mtcars), mtcars[, "carb"] %% mtcars))
    checkDuckDBTable(3.14 %/% tbl, cbind(model = rownames(mtcars), 3.14 %/% mtcars))
})

test_that("Compare methods work as expected for a DuckDBTable", {
    tbl <- DuckDBTable(titanic_parquet, datacols = "fate", keycols = c("Class", "Sex", "Age", "Survived"))

    checkDuckDBTable(tbl == sqrt(tbl), cbind(titanic_df[,1:4], fate = titanic_df$fate == sqrt(titanic_df$fate)))
    checkDuckDBTable(tbl > 1L, cbind(titanic_df[,1:4], fate = titanic_df$fate > 1L))
    checkDuckDBTable(tbl < 3.14, cbind(titanic_df[,1:4], fate = titanic_df$fate < 3.14))
    checkDuckDBTable(1L != tbl, cbind(titanic_df[,1:4], fate = 1L != titanic_df$fate))
    checkDuckDBTable(3.14 <= tbl, cbind(titanic_df[,1:4], fate = 3.14 <= titanic_df$fate))
    checkDuckDBTable(tbl >= sqrt(tbl), cbind(titanic_df[,1:4], fate = titanic_df$fate >= sqrt(titanic_df$fate)))
})

test_that("Logic methods work as expected for a DuckDBTable", {
    tbl <- DuckDBTable(titanic_parquet, datacols = "fate", keycols = c("Class", "Sex", "Age", "Survived"))

    ## "&"
    x <- tbl > 70
    y <- tbl < 4000
    checkDuckDBTable(x & y, cbind(titanic_df[,1:4], fate = titanic_df$fate > 70 & titanic_df$fate < 4000))

    ## "|"
    x <- tbl > 70
    y <- sqrt(tbl) > 0
    checkDuckDBTable(x | y, cbind(titanic_df[,1:4], fate = titanic_df$fate > 70 | sqrt(titanic_df$fate) > 0))
})

test_that("Math methods work as expected for a DuckDBTable", {
    tbl <- DuckDBTable(state_path, datacols = "value", keycols = c("region", "division", "dim1", "dim2"), dimtbls = state_tables)

    income <- tbl[list(dim2 = "Income"), ]
    income_df <- state_df[state_df$dim2 == "Income", ]

    ikeep <-
      c("Colorado", "Delaware", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas",
        "Maine", "Maryland", "Michigan", "Minnesota", "Missouri", "Montana",
        "Nebraska", "Nevada", "New Hampshire", "North Dakota", "Ohio", "Oregon",
        "Pennsylvania", "South Dakota", "Utah", "Vermont", "Washington", "Wisconsin",
        "Wyoming")
    illiteracy <- tbl[list(dim1 = ikeep, dim2 = "Illiteracy"), ]
    illiteracy_df <- state_df[state_df$dim1 %in% ikeep & state_df$dim2 == "Illiteracy", ]

    checkDuckDBTable(abs(income), cbind(income_df[, 1:4], value = abs(income_df$value)))
    checkDuckDBTable(sign(income), cbind(income_df[, 1:4], value = sign(income_df$value)))
    checkDuckDBTable(sqrt(income), cbind(income_df[, 1:4], value = sqrt(income_df$value)))
    checkDuckDBTable(ceiling(income), cbind(income_df[, 1:4], value = ceiling(income_df$value)))
    checkDuckDBTable(floor(income), cbind(income_df[, 1:4], value = floor(income_df$value)))
    checkDuckDBTable(trunc(income), cbind(income_df[, 1:4], value = trunc(income_df$value)))

    expect_error(cummax(income))
    expect_error(cummin(income))
    expect_error(cumprod(income))
    expect_error(cumsum(income))

    checkDuckDBTable(log(income), cbind(income_df[, 1:4], value = log(income_df$value)))
    checkDuckDBTable(log10(income), cbind(income_df[, 1:4], value = log10(income_df$value)))
    checkDuckDBTable(log2(income), cbind(income_df[, 1:4], value = log2(income_df$value)))

    expect_error(log1p(income))

    checkDuckDBTable(acos(illiteracy), cbind(illiteracy_df[, 1:4], value = acos(illiteracy_df$value)))
    checkDuckDBTable(acosh(income), cbind(income_df[, 1:4], value = acosh(income_df$value)))
    checkDuckDBTable(asin(illiteracy), cbind(illiteracy_df[, 1:4], value = asin(illiteracy_df$value)))
    checkDuckDBTable(asinh(income), cbind(income_df[, 1:4], value = asinh(income_df$value)))
    checkDuckDBTable(atan(income), cbind(income_df[, 1:4], value = atan(income_df$value)))
    checkDuckDBTable(atanh(illiteracy), cbind(illiteracy_df[, 1:4], value = atanh(illiteracy_df$value)))

    checkDuckDBTable(exp(income), cbind(income_df[, 1:4], value = exp(income_df$value)))

    expect_error(expm1(income))

    checkDuckDBTable(cos(illiteracy), cbind(illiteracy_df[, 1:4], value = cos(illiteracy_df$value)))
    checkDuckDBTable(cosh(illiteracy), cbind(illiteracy_df[, 1:4], value = cosh(illiteracy_df$value)))

    expect_error(cospi(illiteracy))

    checkDuckDBTable(sin(illiteracy), cbind(illiteracy_df[, 1:4], value = sin(illiteracy_df$value)))
    checkDuckDBTable(sinh(illiteracy), cbind(illiteracy_df[, 1:4], value = sinh(illiteracy_df$value)))

    expect_error(sinpi(illiteracy))

    checkDuckDBTable(tan(illiteracy), cbind(illiteracy_df[, 1:4], value = tan(illiteracy_df$value)))
    checkDuckDBTable(tanh(illiteracy), cbind(illiteracy_df[, 1:4], value = tanh(illiteracy_df$value)))

    expect_error(tanpi(illiteracy))

    checkDuckDBTable(gamma(illiteracy), cbind(illiteracy_df[, 1:4], value = gamma(illiteracy_df$value)))
    checkDuckDBTable(lgamma(illiteracy), cbind(illiteracy_df[, 1:4], value = lgamma(illiteracy_df$value)))

    expect_error(digamma(illiteracy))
    expect_error(trigamma(illiteracy))
})

test_that("Special numeric functions work as expected for a DuckDBTable", {
    tbl <- DuckDBTable(special_path, datacols = "x", keycols = list(id = letters[1:4]))

    expected <- as.data.frame(tbl)
    expected$x <- is.finite(expected$x)
    checkDuckDBTable(is.finite(tbl), expected)

    expected <- as.data.frame(tbl)
    expected$x <- is.infinite(expected$x)
    checkDuckDBTable(is.infinite(tbl), expected)

    expected <- as.data.frame(tbl)
    expected$x <- is.nan(expected$x)
    checkDuckDBTable(is.nan(tbl), expected)
})

test_that("Summary methods work as expected for a DuckDBTable", {
    tbl <- DuckDBTable(titanic_parquet, datacols = "fate", keycols = c("Class", "Sex", "Age", "Survived"))
    expect_identical(max(tbl), max(as.data.frame(tbl)[["fate"]]))
    expect_identical(min(tbl), min(as.data.frame(tbl)[["fate"]]))
    expect_identical(range(tbl), range(as.data.frame(tbl)[["fate"]]))
    expect_equal(prod(tbl), prod(as.data.frame(tbl)[["fate"]]))
    expect_equal(sum(tbl), sum(as.data.frame(tbl)[["fate"]]))
    expect_identical(any(tbl == 0L), any(as.data.frame(tbl)[["fate"]] == 0L))
    expect_identical(all(tbl == 0L), all(as.data.frame(tbl)[["fate"]] == 0L))
})

test_that("Other aggregate methods work as expected for a DuckDBTable", {
    tbl <- DuckDBTable(titanic_parquet, datacols = "fate", keycols = c("Class", "Sex", "Age", "Survived"))
    expect_equal(mean(tbl), mean(as.data.frame(tbl)[["fate"]]))
    expect_equal(median(tbl), median(as.data.frame(tbl)[["fate"]]))
    expect_equal(var(tbl), var(as.data.frame(tbl)[["fate"]]))
    expect_equal(sd(tbl), sd(as.data.frame(tbl)[["fate"]]))
    expect_equal(mad(tbl), mad(as.data.frame(tbl)[["fate"]]))
    expect_equal(mad(tbl, constant = 1), mad(as.data.frame(tbl)[["fate"]], constant = 1))

    expect_equal(quantile(tbl), quantile(as.data.frame(tbl)[["fate"]]))
    expect_equal(quantile(tbl, probs = seq(0, 1, by = 0.05)), quantile(as.data.frame(tbl)[["fate"]], probs = seq(0, 1, by = 0.05)))
    expect_equal(quantile(tbl, names = FALSE), quantile(as.data.frame(tbl)[["fate"]], names = FALSE))
    expect_equal(quantile(tbl, type = 1), quantile(as.data.frame(tbl)[["fate"]], type = 1))

    expect_equal(IQR(tbl), IQR(as.data.frame(tbl)[["fate"]]))
    expect_equal(IQR(tbl, type = 1), IQR(as.data.frame(tbl)[["fate"]], type = 1))

    tbl <- DuckDBTable(mtcars_parquet, datacols = c("cyl", "vs", "am", "gear", "carb"), keycols = "model")
    expect_equal(table(tbl), table(as.data.frame(tbl)[-1L]))
})
