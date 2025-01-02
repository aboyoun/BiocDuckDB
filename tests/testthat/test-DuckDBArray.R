# Tests the basic functions of a DuckDBArray.
# library(testthat); library(BiocDuckDB); source("setup.R"); source("test-DuckDBArray.R")

test_that("basic methods work as expected for a DuckDBArray", {
    pqarray <- DuckDBArray(titanic_parquet, datacols = "fate", keycols = dimnames(titanic_array))
    checkDuckDBArray(pqarray, titanic_array)
    expect_true(is_sparse(pqarray))

    pqarray <- DuckDBArray(titanic_parquet, datacols = "fate", keycols = dimnames(titanic_array), type = "double")
    expect_s4_class(pqarray, "DuckDBArray")
    expect_identical(type(pqarray), "double")
    expect_identical(length(pqarray), length(titanic_array))
    expect_identical(dim(pqarray), dim(titanic_array))
    expect_identical(dimnames(pqarray), dimnames(titanic_array))
    expect_equal(as.array(pqarray), titanic_array)

    pqarray <- DuckDBArray(titanic_parquet, datacols = "fate", keycols = dimnames(titanic_array), type = "character")
    expect_s4_class(pqarray, "DuckDBArray")
    expect_identical(type(pqarray), "character")
    expect_identical(length(pqarray), length(titanic_array))
    expect_identical(dim(pqarray), dim(titanic_array))
    expect_identical(dimnames(pqarray), dimnames(titanic_array))
})

test_that("basic methods work as expected for a sparse DuckDBArray", {
    pqarray <- DuckDBArray(titanic_parquet, datacols = "fate",
                           keycols = list(Class = c("1st", "2nd", "Crew"),
                                          Sex = c("Male", "Female"),
                                          Age = "Child", Survived = "No"))
    checkDuckDBArray(pqarray, titanic_array[c("1st", "2nd", "Crew"), c("Male", "Female"), "Child", "No", drop = FALSE])
    expect_true(is_sparse(pqarray))

    pqarray <- DuckDBArray(titanic_parquet, datacols = "fate",
                           keycols = list(Class = c("1st", "2nd", "3rd", "Crew"),
                                          Sex = c("Male", "Female"),
                                          Age = "Child", Survived = "No"))
    checkDuckDBArray(pqarray, titanic_array[, , "Child", "No", drop = FALSE])
    expect_true(is_sparse(pqarray))

    pqarray <- DuckDBArray(sparse_parquet, datacols = "value", keycols = list(dim1 = LETTERS, dim2 = letters, dim3 = month.abb))
    checkDuckDBArray(pqarray, sparse_array)
    expect_true(is_sparse(pqarray))

    pqarray <- DuckDBArray(sparse_parquet, datacols = "value", keycols = list(dim1 = LETTERS, dim2 = letters, dim3 = month.abb), type = "double")
    expect_s4_class(pqarray, "DuckDBArray")
    expect_identical(type(pqarray), "double")
    expect_identical(length(pqarray), length(sparse_array))
    expect_identical(dim(pqarray), dim(sparse_array))
    expect_identical(dimnames(pqarray), dimnames(sparse_array))
    expect_equal(as.array(pqarray), sparse_array)

    pqarray <- DuckDBArray(sparse_parquet, datacols = "value", keycols = list(dim1 = LETTERS, dim2 = letters, dim3 = month.abb), type = "character")
    expect_s4_class(pqarray, "DuckDBArray")
    expect_identical(type(pqarray), "character")
    expect_identical(length(pqarray), length(sparse_array))
    expect_identical(dim(pqarray), dim(sparse_array))
    expect_identical(dimnames(pqarray), dimnames(sparse_array))
})

test_that("DuckDBArray can be cast to a different type", {
    pqarray <- DuckDBArray(titanic_parquet, datacols = "fate", keycols = dimnames(titanic_array))
    type(pqarray) <- "double"
    expected <- titanic_array
    storage.mode(expected) <- "double"
    checkDuckDBArray(pqarray, expected)

    pqarray <- DuckDBArray(sparse_parquet, datacols = "value", keycols = list(dim1 = LETTERS, dim2 = letters, dim3 = month.abb))
    type(pqarray) <- "double"
    expected <- sparse_array
    storage.mode(expected) <- "double"
    checkDuckDBArray(pqarray, expected)
})

test_that("nonzero functions work for DuckDBArray", {
    pqarray <- DuckDBArray(titanic_parquet, datacols = "fate", keycols = dimnames(titanic_array))
    checkDuckDBArray(is_nonzero(pqarray), is_nonzero(titanic_array))
    expect_equal(nzcount(pqarray), nzcount(titanic_array))

    pqarray <- DuckDBArray(titanic_parquet, datacols = "fate",
                           keycols = list(Class = c("1st", "2nd", "Crew"),
                                          Sex = c("Male", "Female"),
                                          Age = "Child", Survived = "No"))
    expected <- titanic_array[c("1st", "2nd", "Crew"), c("Male", "Female"), "Child", "No", drop = FALSE]
    checkDuckDBArray(is_nonzero(pqarray), is_nonzero(expected))
    expect_equal(nzcount(pqarray), nzcount(expected))

    pqarray <- DuckDBArray(titanic_parquet, datacols = "fate",
                           keycols = list(Class = c("1st", "2nd", "3rd", "Crew"),
                                          Sex = c("Male", "Female"),
                                          Age = "Child", Survived = "No"))
    expected <- titanic_array[, , "Child", "No", drop = FALSE]
    checkDuckDBArray(pqarray, expected)
    expect_equal(nzcount(pqarray), nzcount(expected))
})

test_that("extraction methods work as expected for a DuckDBArray", {
    pqarray <- DuckDBArray(titanic_parquet, datacols = "fate", keycols = dimnames(titanic_array))

    expect_error(pqarray[,])

    object <- pqarray[]
    checkDuckDBArray(object, titanic_array)

    object <- pqarray[, 2:1, , ]
    expected <- titanic_array[, 2:1, , ]
    checkDuckDBArray(object, expected)

    object <- pqarray[c(4, 2), , 1, ]
    expected <- titanic_array[c(4, 2), , 1, ]
    checkDuckDBArray(object, expected)

    object <- pqarray[c(4, 2), , 1, , drop = FALSE]
    expected <- titanic_array[c(4, 2), , 1, , drop = FALSE]
    checkDuckDBArray(object, expected)

    object <- pqarray[4, 2, 1, 2]
    expected <- as.array(titanic_array[4, 2, 1, 2])
    checkDuckDBArray(object, expected)

    object <- pqarray[4, 2, 1, 2, drop = FALSE]
    expected <- titanic_array[4, 2, 1, 2, drop = FALSE]
    checkDuckDBArray(object, expected)

    object <- pqarray[c("1st", "2nd", "3rd"), "Female", "Child", ]
    expected <- titanic_array[c("1st", "2nd", "3rd"), "Female", "Child", ]
    checkDuckDBArray(object, expected)
})

test_that("aperm and t methods work as expected for a DuckDBArray", {
    seed <- DuckDBArray(titanic_parquet, datacols = "fate", keycols = dimnames(titanic_array))

    object <- aperm(seed, c(4, 2, 1, 3))
    expected <- aperm(titanic_array, c(4, 2, 1, 3))
    checkDuckDBArray(object, expected)

    names(dimnames(state.x77)) <- c("rowname", "colname")
    seed <- DuckDBArray(state_path, datacols = "value", keycols = dimnames(state.x77))

    object <- t(seed)
    expected <- t(state.x77)
    expect_s4_class(object, "DuckDBArray")
    expect_identical(type(object), "double")
    expect_identical(length(object), length(expected))
    expect_identical(dim(object), dim(expected))
    expect_identical(dimnames(object)[[1L]], dimnames(expected)[[1L]])
    expect_setequal(dimnames(object)[[2L]], dimnames(expected)[[2L]])
    expect_identical(as.array(object)[, colnames(expected)], expected)
})

test_that("Arith methods work as expected for a DuckDBArray", {
    pqarray <- DuckDBArray(titanic_parquet, datacols = "fate", keycols = dimnames(titanic_array))

    checkDuckDBArray(pqarray + sqrt(pqarray), as.array(pqarray) + sqrt(as.array(pqarray)))
    checkDuckDBArray(pqarray - 1L, as.array(pqarray) - 1L)
    checkDuckDBArray(pqarray * 3.14, as.array(pqarray) * 3.14)
    checkDuckDBArray(1L / pqarray, 1L / as.array(pqarray))
    checkDuckDBArray(3.14 ^ pqarray, 3.14 ^ as.array(pqarray))
    checkDuckDBArray(pqarray %% sqrt(pqarray), as.array(pqarray) %% sqrt(as.array(pqarray)))
    checkDuckDBArray(pqarray %/% 3.14, as.array(pqarray) %/% 3.14)
})

test_that("Compare methods work as expected for a DuckDBArray", {
    pqarray <- DuckDBArray(titanic_parquet, datacols = "fate", keycols = dimnames(titanic_array))

    checkDuckDBArray(pqarray == sqrt(pqarray), as.array(pqarray) == sqrt(as.array(pqarray)))
    checkDuckDBArray(pqarray > 1L, as.array(pqarray) > 1L)
    checkDuckDBArray(pqarray < 3.14, as.array(pqarray) < 3.14)
    checkDuckDBArray(1L != pqarray, 1L != as.array(pqarray))
    checkDuckDBArray(3.14 <= pqarray, 3.14 <= as.array(pqarray))
    checkDuckDBArray(pqarray >= sqrt(pqarray), as.array(pqarray) >= sqrt(as.array(pqarray)))
})

test_that("Logic methods work as expected for a DuckDBArray", {
    pqarray <- DuckDBArray(titanic_parquet, datacols = "fate", keycols = dimnames(titanic_array))

    ## "&"
    x <- pqarray > 70
    y <- pqarray < 4000
    checkDuckDBArray(x & y, as.array(x) & as.array(y))

    ## "|"
    x <- pqarray > 70
    y <- sqrt(pqarray) > 0
    checkDuckDBArray(x | y, as.array(x) | as.array(y))
})

test_that("Math methods work as expected for a DuckDBArray", {
    names(dimnames(state.x77)) <- c("rowname", "colname")
    pqarray <- DuckDBArray(state_path, datacols = "value", keycols = dimnames(state.x77))

    income <- pqarray[, "Income"]
    ikeep <-
      c("Colorado", "Delaware", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas",
        "Maine", "Maryland", "Michigan", "Minnesota", "Missouri", "Montana",
        "Nebraska", "Nevada", "New Hampshire", "North Dakota", "Ohio", "Oregon",
        "Pennsylvania", "South Dakota", "Utah", "Vermont", "Washington", "Wisconsin",
        "Wyoming")
    illiteracy <- pqarray[ikeep, "Illiteracy"]

    checkDuckDBArray(abs(income), abs(as.array(income)))
    checkDuckDBArray(sqrt(income), sqrt(as.array(income)))
    checkDuckDBArray(ceiling(income), ceiling(as.array(income)))
    checkDuckDBArray(floor(income), floor(as.array(income)))
    checkDuckDBArray(trunc(income), trunc(as.array(income)))

    expect_error(cummax(income))
    expect_error(cummin(income))
    expect_error(cumprod(income))
    expect_error(cumsum(income))

    checkDuckDBArray(log(income), log(as.array(income)))
    checkDuckDBArray(log10(income), log10(as.array(income)))
    checkDuckDBArray(log2(income), log2(as.array(income)))

    expect_error(log1p(income))

    checkDuckDBArray(acos(illiteracy), acos(as.array(illiteracy)))
    checkDuckDBArray(acosh(income), acosh(as.array(income)))
    checkDuckDBArray(asin(illiteracy), asin(as.array(illiteracy)))
    checkDuckDBArray(asinh(income), asinh(as.array(income)))
    checkDuckDBArray(atan(income), atan(as.array(income)))
    checkDuckDBArray(atanh(illiteracy), atanh(as.array(illiteracy)))

    checkDuckDBArray(exp(income), exp(as.array(income)))

    expect_error(expm1(income))

    checkDuckDBArray(cos(illiteracy), cos(as.array(illiteracy)))
    checkDuckDBArray(cosh(illiteracy), cosh(as.array(illiteracy)))

    expect_error(cospi(illiteracy))

    checkDuckDBArray(sin(illiteracy), sin(as.array(illiteracy)))
    checkDuckDBArray(sinh(illiteracy), sinh(as.array(illiteracy)))

    expect_error(sinpi(illiteracy))

    checkDuckDBArray(tan(illiteracy), tan(as.array(illiteracy)))
    checkDuckDBArray(tanh(illiteracy), tanh(as.array(illiteracy)))

    expect_error(tanpi(illiteracy))

    checkDuckDBArray(gamma(illiteracy), gamma(as.array(illiteracy)))
    checkDuckDBArray(lgamma(illiteracy), lgamma(as.array(illiteracy)))

    expect_error(digamma(illiteracy))
    expect_error(trigamma(illiteracy))
})

test_that("Special numeric functions work as expected for a DuckDBArray", {
    pqarray <- DuckDBArray(special_path, datacols = "x", keycols = list(id = letters[1:4]))

    checkDuckDBArray(is.finite(pqarray), is.finite(as.array(pqarray)))
    checkDuckDBArray(is.infinite(pqarray), is.infinite(as.array(pqarray)))
    checkDuckDBArray(is.nan(pqarray), is.nan(as.array(pqarray)))
})

test_that("row/colSums methods work as expected for a DuckDBArray", {
    pqarray <- DuckDBArray(titanic_parquet, datacols = "fate", keycols = dimnames(titanic_array))

    object <- rowSums(pqarray)
    expect_identical(setNames(as.vector(object), names(object)), rowSums(as.array(pqarray)))
    object <- rowSums(pqarray, dims = 2L)
    expect_identical(setNames(object, names(object)), rowSums(as.array(pqarray), dims = 2L))
    object <- rowSums(pqarray, dims = 3L)
    expect_identical(setNames(object, names(object)), rowSums(as.array(pqarray), dims = 3L))

    object <- colSums(pqarray)
    expect_identical(setNames(object, names(object)), colSums(as.array(pqarray)))
    object <- colSums(pqarray, dims = 2L)
    expect_identical(setNames(object, names(object)), colSums(as.array(pqarray), dims = 2L))
    object <- colSums(pqarray, dims = 3L)
    expect_identical(setNames(as.vector(object), names(object)), colSums(as.array(pqarray), dims = 3L))
})
