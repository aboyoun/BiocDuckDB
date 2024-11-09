# Tests the basic functions of a DuckDBArraySeed.
# library(testthat); library(BiocDuckDB); source("setup.R"); source("test-DuckDBArraySeed.R")

test_that("basic methods work as expected for a DuckDBArraySeed", {
    seed <- DuckDBArraySeed(titanic_parquet, keycols = dimnames(titanic_array), datacols = "fate")
    checkDuckDBArraySeed(seed, titanic_array)
    expect_false(is_sparse(seed))

    seed <- DuckDBArraySeed(titanic_parquet, keycols = dimnames(titanic_array), datacols = "fate", type = "double")
    expect_s4_class(seed, "DuckDBArraySeed")
    expect_identical(type(seed), "double")
    expect_identical(length(seed), length(titanic_array))
    expect_identical(dim(seed), dim(titanic_array))
    expect_identical(dimnames(seed), dimnames(titanic_array))
    expect_equal(as.array(seed), titanic_array)

    seed <- DuckDBArraySeed(titanic_parquet, keycols = dimnames(titanic_array), datacols = "fate", type = "character")
    expect_s4_class(seed, "DuckDBArraySeed")
    expect_identical(type(seed), "character")
    expect_identical(length(seed), length(titanic_array))
    expect_identical(dim(seed), dim(titanic_array))
    expect_identical(dimnames(seed), dimnames(titanic_array))
})

test_that("basic methods work as expected for a sparse DuckDBArraySeed", {
    seed <- DuckDBArraySeed(sparse_parquet, keycols = list(dim1 = LETTERS, dim2 = letters, dim3 = month.abb), datacols = "value")
    checkDuckDBArraySeed(seed, sparse_array)
    expect_true(is_sparse(seed))

    seed <- DuckDBArraySeed(sparse_parquet, keycols = list(dim1 = LETTERS, dim2 = letters, dim3 = month.abb), datacols = "value", type = "double")
    expect_s4_class(seed, "DuckDBArraySeed")
    expect_identical(type(seed), "double")
    expect_identical(length(seed), length(sparse_array))
    expect_identical(dim(seed), dim(sparse_array))
    expect_identical(dimnames(seed), dimnames(sparse_array))
    expect_equal(as.array(seed), sparse_array)

    seed <- DuckDBArraySeed(sparse_parquet, keycols = list(dim1 = LETTERS, dim2 = letters, dim3 = month.abb), datacols = "value", type = "character")
    expect_s4_class(seed, "DuckDBArraySeed")
    expect_identical(type(seed), "character")
    expect_identical(length(seed), length(sparse_array))
    expect_identical(dim(seed), dim(sparse_array))
    expect_identical(dimnames(seed), dimnames(sparse_array))
})

test_that("DuckDBArraySeed can be cast to a different type", {
    seed <- DuckDBArraySeed(titanic_parquet, keycols = dimnames(titanic_array), datacols = "fate")
    type(seed) <- "double"
    expected <- titanic_array
    storage.mode(expected) <- "double"
    checkDuckDBArraySeed(seed, expected)

    seed <- DuckDBArraySeed(sparse_parquet, keycols = list(dim1 = LETTERS, dim2 = letters, dim3 = month.abb), datacols = "value")
    type(seed) <- "double"
    expected <- sparse_array
    storage.mode(expected) <- "double"
    checkDuckDBArraySeed(seed, expected)
})

test_that("nonzero functions work for DuckDBArraySeed", {
    seed <- DuckDBArraySeed(titanic_parquet, keycols = dimnames(titanic_array), datacols = "fate")
    checkDuckDBArraySeed(is_nonzero(seed), is_nonzero(titanic_array))
    expect_equal(nzcount(seed), nzcount(titanic_array))

    seed <- DuckDBArraySeed(sparse_parquet, keycols = list(dim1 = LETTERS, dim2 = letters, dim3 = month.abb), datacols = "value")
    checkDuckDBArraySeed(is_nonzero(seed), is_nonzero(sparse_array))
    expect_equal(nzcount(seed), nzcount(sparse_array))
})

test_that("extraction methods work as expected for a DuckDBArraySeed", {
    seed <- DuckDBArraySeed(titanic_parquet, keycols = dimnames(titanic_array), datacols = "fate")

    expect_error(seed[,])

    object <- seed[]
    checkDuckDBArraySeed(object, titanic_array)

    object <- seed[, 2:1, , ]
    expected <- titanic_array[, 2:1, , ]
    checkDuckDBArraySeed(object, expected)

    object <- seed[c(4, 2), , 1, ]
    expected <- titanic_array[c(4, 2), , 1, ]
    checkDuckDBArraySeed(object, expected)

    object <- seed[c(4, 2), , 1, , drop = FALSE]
    expected <- titanic_array[c(4, 2), , 1, , drop = FALSE]
    checkDuckDBArraySeed(object, expected)

    object <- seed[4, 2, 1, 2]
    expected <- as.array(titanic_array[4, 2, 1, 2])
    checkDuckDBArraySeed(object, expected)

    object <- seed[4, 2, 1, 2, drop = FALSE]
    expected <- titanic_array[4, 2, 1, 2, drop = FALSE]
    checkDuckDBArraySeed(object, expected)

    object <- seed[c("1st", "2nd", "3rd"), "Female", "Child", ]
    expected <- titanic_array[c("1st", "2nd", "3rd"), "Female", "Child", ]
    checkDuckDBArraySeed(object, expected)
})

test_that("aperm and t methods work as expected for a DuckDBArraySeed", {
    seed <- DuckDBArraySeed(titanic_parquet, keycols = dimnames(titanic_array), datacols = "fate")

    object <- aperm(seed, c(4, 2, 1, 3))
    expected <- aperm(titanic_array, c(4, 2, 1, 3))
    checkDuckDBArraySeed(object, expected)

    names(dimnames(state.x77)) <- c("rowname", "colname")
    seed <- DuckDBArraySeed(state_path, keycols = dimnames(state.x77), datacols = "value")

    object <- t(seed)
    expected <- t(state.x77)
    expect_s4_class(object, "DuckDBArraySeed")
    expect_identical(type(object), "double")
    expect_identical(length(object), length(expected))
    expect_identical(dim(object), dim(expected))
    expect_identical(dimnames(object)[[1L]], dimnames(expected)[[1L]])
    expect_setequal(dimnames(object)[[2L]], dimnames(expected)[[2L]])
    expect_identical(as.array(object)[, colnames(expected)], expected)
})

test_that("Arith methods work as expected for a DuckDBArraySeed", {
    seed <- DuckDBArraySeed(titanic_parquet, keycols = dimnames(titanic_array), datacols = "fate")

    checkDuckDBArraySeed(seed + sqrt(seed), as.array(seed) + sqrt(as.array(seed)))
    checkDuckDBArraySeed(seed - 1L, as.array(seed) - 1L)
    checkDuckDBArraySeed(seed * 3.14, as.array(seed) * 3.14)
    checkDuckDBArraySeed(1L / seed, 1L / as.array(seed))
    checkDuckDBArraySeed(3.14 ^ seed, 3.14 ^ as.array(seed))
    checkDuckDBArraySeed(seed %% sqrt(seed), as.array(seed) %% sqrt(as.array(seed)))
    checkDuckDBArraySeed(seed %/% 3.14, as.array(seed) %/% 3.14)
})

test_that("Compare methods work as expected for a DuckDBArraySeed", {
    seed <- DuckDBArraySeed(titanic_parquet, keycols = dimnames(titanic_array), datacols = "fate")

    checkDuckDBArraySeed(seed == sqrt(seed), as.array(seed) == sqrt(as.array(seed)))
    checkDuckDBArraySeed(seed > 1L, as.array(seed) > 1L)
    checkDuckDBArraySeed(seed < 3.14, as.array(seed) < 3.14)
    checkDuckDBArraySeed(1L != seed, 1L != as.array(seed))
    checkDuckDBArraySeed(3.14 <= seed, 3.14 <= as.array(seed))
    checkDuckDBArraySeed(seed >= sqrt(seed), as.array(seed) >= sqrt(as.array(seed)))
})

test_that("Logic methods work as expected for a DuckDBArraySeed", {
    seed <- DuckDBArraySeed(titanic_parquet, keycols = dimnames(titanic_array), datacols = "fate")

    ## "&"
    x <- seed > 70
    y <- seed < 4000
    checkDuckDBArraySeed(x & y, as.array(x) & as.array(y))

    ## "|"
    x <- seed > 70
    y <- sqrt(seed) > 0
    checkDuckDBArraySeed(x | y, as.array(x) | as.array(y))
})

test_that("Math methods work as expected for a DuckDBArraySeed", {
    names(dimnames(state.x77)) <- c("rowname", "colname")
    seed <- DuckDBArraySeed(state_path, keycols = dimnames(state.x77), datacols = "value")

    income <- seed[, "Income"]
    ikeep <-
      c("Colorado", "Delaware", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas",
        "Maine", "Maryland", "Michigan", "Minnesota", "Missouri", "Montana",
        "Nebraska", "Nevada", "New Hampshire", "North Dakota", "Ohio", "Oregon",
        "Pennsylvania", "South Dakota", "Utah", "Vermont", "Washington", "Wisconsin",
        "Wyoming")
    illiteracy <- seed[ikeep, "Illiteracy"]

    checkDuckDBArraySeed(abs(income), abs(as.array(income)))
    checkDuckDBArraySeed(sqrt(income), sqrt(as.array(income)))
    checkDuckDBArraySeed(ceiling(income), ceiling(as.array(income)))
    checkDuckDBArraySeed(floor(income), floor(as.array(income)))
    checkDuckDBArraySeed(trunc(income), trunc(as.array(income)))

    expect_error(cummax(income))
    expect_error(cummin(income))
    expect_error(cumprod(income))
    expect_error(cumsum(income))

    checkDuckDBArraySeed(log(income), log(as.array(income)))
    checkDuckDBArraySeed(log10(income), log10(as.array(income)))
    checkDuckDBArraySeed(log2(income), log2(as.array(income)))

    expect_error(log1p(income))

    checkDuckDBArraySeed(acos(illiteracy), acos(as.array(illiteracy)))
    checkDuckDBArraySeed(acosh(income), acosh(as.array(income)))
    checkDuckDBArraySeed(asin(illiteracy), asin(as.array(illiteracy)))
    checkDuckDBArraySeed(asinh(income), asinh(as.array(income)))
    checkDuckDBArraySeed(atan(income), atan(as.array(income)))
    checkDuckDBArraySeed(atanh(illiteracy), atanh(as.array(illiteracy)))

    checkDuckDBArraySeed(exp(income), exp(as.array(income)))

    expect_error(expm1(income))

    checkDuckDBArraySeed(cos(illiteracy), cos(as.array(illiteracy)))
    checkDuckDBArraySeed(cosh(illiteracy), cosh(as.array(illiteracy)))

    expect_error(cospi(illiteracy))

    checkDuckDBArraySeed(sin(illiteracy), sin(as.array(illiteracy)))
    checkDuckDBArraySeed(sinh(illiteracy), sinh(as.array(illiteracy)))

    expect_error(sinpi(illiteracy))

    checkDuckDBArraySeed(tan(illiteracy), tan(as.array(illiteracy)))
    checkDuckDBArraySeed(tanh(illiteracy), tanh(as.array(illiteracy)))

    expect_error(tanpi(illiteracy))

    checkDuckDBArraySeed(gamma(illiteracy), gamma(as.array(illiteracy)))
    checkDuckDBArraySeed(lgamma(illiteracy), lgamma(as.array(illiteracy)))

    expect_error(digamma(illiteracy))
    expect_error(trigamma(illiteracy))
})

test_that("Summary methods work as expected for a DuckDBArraySeed", {
    seed <- DuckDBArraySeed(titanic_parquet, keycols = dimnames(titanic_array), datacols = "fate")
    expect_identical(max(seed), max(as.array(seed)))
    expect_identical(min(seed), min(as.array(seed)))
    expect_identical(range(seed), range(as.array(seed)))
    expect_equal(prod(seed), prod(as.array(seed)))
    expect_equal(sum(seed), sum(as.array(seed)))
    expect_identical(any(seed == 0L), any(as.array(seed) == 0L))
    expect_identical(all(seed == 0L), all(as.array(seed) == 0L))
})

test_that("Other aggregate methods work as expected for a DuckDBArraySeed", {
    seed <- DuckDBArraySeed(titanic_parquet, keycols = dimnames(titanic_array), datacols = "fate")
    expect_equal(mean(seed), mean(as.array(seed)))
    expect_equal(median(seed), median(as.array(seed)))
    expect_equal(var(seed), var(as.array(seed)))
    expect_equal(sd(seed), sd(as.array(seed)))
    expect_equal(mad(seed), mad(as.array(seed)))
    expect_equal(mad(seed, constant = 1), mad(as.array(seed), constant = 1))

    expect_equal(quantile(seed), quantile(as.array(seed)))
    expect_equal(quantile(seed, probs = seq(0, 1, by = 0.05)), quantile(as.array(seed), probs = seq(0, 1, by = 0.05)))
    expect_equal(quantile(seed, names = FALSE), quantile(as.array(seed), names = FALSE))
    expect_equal(quantile(seed, type = 1), quantile(as.array(seed), type = 1))

    expect_equal(IQR(seed), IQR(as.array(seed)))
    expect_equal(IQR(seed, type = 1), IQR(as.array(seed), type = 1))
})
