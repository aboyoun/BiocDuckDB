# Tests the basic functions of a DuckDBMatrix.
# library(testthat); library(BiocDuckDB); source("setup.R"); source("test-DuckDBMatrix.R")

test_that("basic methods work as expected for a DuckDBMatrix", {
    names(dimnames(state.x77)) <- c("rowname", "colname")

    pqmat <- DuckDBMatrix(state_path, datacol = "value", row = "rowname", col = "colname")
    expect_s4_class(pqmat, "DuckDBMatrix")
    expect_identical(type(pqmat), "double")
    expect_identical(type(pqmat), typeof(state.x77))
    expect_identical(length(pqmat), length(state.x77))
    expect_identical(dim(pqmat), dim(state.x77))
    expect_setequal(rownames(pqmat), rownames(state.x77))
    expect_setequal(colnames(pqmat), colnames(state.x77))
    expect_equal(as.matrix(pqmat)[rownames(state.x77), colnames(state.x77)], state.x77)

    pqmat <- DuckDBMatrix(state_path, datacol = "value", row = dimnames(state.x77)[1L], col = "colname")
    expect_s4_class(pqmat, "DuckDBMatrix")
    expect_identical(type(pqmat), "double")
    expect_identical(type(pqmat), typeof(state.x77))
    expect_identical(length(pqmat), length(state.x77))
    expect_identical(dim(pqmat), dim(state.x77))
    expect_setequal(rownames(pqmat), rownames(state.x77))
    expect_setequal(colnames(pqmat), colnames(state.x77))
    expect_equal(as.matrix(pqmat)[rownames(state.x77), colnames(state.x77)], state.x77)

    pqmat <- DuckDBMatrix(state_path, datacol = "value", row = "rowname", col = dimnames(state.x77)[2L])
    expect_s4_class(pqmat, "DuckDBMatrix")
    expect_identical(type(pqmat), "double")
    expect_identical(type(pqmat), typeof(state.x77))
    expect_identical(length(pqmat), length(state.x77))
    expect_identical(dim(pqmat), dim(state.x77))
    expect_setequal(rownames(pqmat), rownames(state.x77))
    expect_setequal(colnames(pqmat), colnames(state.x77))
    expect_equal(as.matrix(pqmat)[rownames(state.x77), colnames(state.x77)], state.x77)

    pqmat <- DuckDBMatrix(state_path, datacol = "value", row = dimnames(state.x77)[1L], col = dimnames(state.x77)[2L])
    checkDuckDBMatrix(pqmat, state.x77)

    pqmat <- DuckDBMatrix(state_path, datacol = "value", keycols = dimnames(state.x77))
    checkDuckDBMatrix(pqmat, state.x77)
})

test_that("extraction methods work as expected for a DuckDBMatrix", {
    names(dimnames(state.x77)) <- c("rowname", "colname")

    pqmat <- DuckDBMatrix(state_path, datacol = "value", row = dimnames(state.x77)[1L], col = dimnames(state.x77)[2L])

    expected <- as.array(state.x77[1, ])
    names(dimnames(expected)) <- "colname"
    checkDuckDBArray(pqmat[1, ], expected)
    checkDuckDBMatrix(pqmat[1, , drop = FALSE], state.x77[1, , drop = FALSE])

    expected <- as.array(state.x77["New Jersey", ])
    names(dimnames(expected)) <- "colname"
    checkDuckDBArray(pqmat["New Jersey", ], expected)
    checkDuckDBMatrix(pqmat["New Jersey", , drop = FALSE], state.x77["New Jersey", , drop = FALSE])

    checkDuckDBMatrix(pqmat[c("New Jersey", "Washington"), ], state.x77[c("New Jersey", "Washington"), ])

    expected <- as.array(state.x77[, 4])
    names(dimnames(expected)) <- "rowname"
    checkDuckDBArray(pqmat[, 4], expected)
    checkDuckDBMatrix(pqmat[, 4, drop = FALSE], state.x77[, 4, drop = FALSE])

    expected <- as.array(state.x77[, "Murder"])
    names(dimnames(expected)) <- "rowname"
    checkDuckDBArray(pqmat[, "Murder"], expected)
    checkDuckDBMatrix(pqmat[, "Murder", drop = FALSE], state.x77[, "Murder", drop = FALSE])

    checkDuckDBMatrix(pqmat[, c("Income", "Life Exp", "Murder")], state.x77[, c("Income", "Life Exp", "Murder")])

    checkDuckDBMatrix(pqmat[c(13, 7), c(1, 3, 5, 7)], state.x77[c(13, 7), c(1, 3, 5, 7)])
    checkDuckDBMatrix(pqmat[c("New Jersey", "Washington"), c("Income", "Life Exp", "Murder")],
                       state.x77[c("New Jersey", "Washington"), c("Income", "Life Exp", "Murder")])
})

test_that("aperm and t methods work as expected for a DuckDBMatrix", {
    names(dimnames(state.x77)) <- c("rowname", "colname")
    pqmat <- DuckDBMatrix(state_path, datacol = "value", row = dimnames(state.x77)[1L], col = dimnames(state.x77)[2L])
    checkDuckDBMatrix(aperm(pqmat, c(2, 1)), aperm(state.x77, c(2, 1)))
    checkDuckDBMatrix(t(pqmat), t(state.x77))
})

test_that("Arith methods work as expected for a DuckDBMatrix", {
    names(dimnames(state.x77)) <- c("rowname", "colname")
    pqmat <- DuckDBMatrix(state_path, datacol = "value", row = dimnames(state.x77)[1L], col = dimnames(state.x77)[2L])

    checkDuckDBMatrix(pqmat + sqrt(pqmat), as.array(pqmat) + sqrt(as.array(pqmat)))
    checkDuckDBMatrix(pqmat - 1L, as.array(pqmat) - 1L)
    checkDuckDBMatrix(pqmat * 3.14, as.array(pqmat) * 3.14)
    checkDuckDBMatrix(1L / pqmat, 1L / as.array(pqmat))
    checkDuckDBMatrix(3.14 ^ pqmat, 3.14 ^ as.array(pqmat))
    checkDuckDBMatrix(pqmat %% sqrt(pqmat), as.array(pqmat) %% sqrt(as.array(pqmat)))
    checkDuckDBMatrix(pqmat %/% 3.14, as.array(pqmat) %/% 3.14)
})

test_that("Compare methods work as expected for a DuckDBMatrix", {
    names(dimnames(state.x77)) <- c("rowname", "colname")
    pqmat <- DuckDBMatrix(state_path, datacol = "value", row = dimnames(state.x77)[1L], col = dimnames(state.x77)[2L])

    checkDuckDBMatrix(pqmat == sqrt(pqmat), as.array(pqmat) == sqrt(as.array(pqmat)))
    checkDuckDBMatrix(pqmat > 1L, as.array(pqmat) > 1L)
    checkDuckDBMatrix(pqmat < 3.14, as.array(pqmat) < 3.14)
    checkDuckDBMatrix(1L != pqmat, 1L != as.array(pqmat))
    checkDuckDBMatrix(3.14 <= pqmat, 3.14 <= as.array(pqmat))
    checkDuckDBMatrix(pqmat >= sqrt(pqmat), as.array(pqmat) >= sqrt(as.array(pqmat)))
})

test_that("Logic methods work as expected for a DuckDBMatrix", {
    names(dimnames(state.x77)) <- c("rowname", "colname")
    pqmat <- DuckDBMatrix(state_path, datacol = "value", row = dimnames(state.x77)[1L], col = dimnames(state.x77)[2L])

    ## "&"
    x <- pqmat > 70
    y <- pqmat < 4000
    checkDuckDBMatrix(x & y, as.array(x) & as.array(y))

    ## "|"
    x <- pqmat > 70
    y <- sqrt(pqmat) > 0
    checkDuckDBMatrix(x | y, as.array(x) | as.array(y))
})

test_that("Math methods work as expected for a DuckDBMatrix", {
    names(dimnames(state.x77)) <- c("rowname", "colname")
    pqmat <- DuckDBMatrix(state_path, datacol = "value", row = dimnames(state.x77)[1L], col = dimnames(state.x77)[2L])

    income <- pqmat[, "Income", drop = FALSE]
    ikeep <-
      c("Colorado", "Delaware", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas",
        "Maine", "Maryland", "Michigan", "Minnesota", "Missouri", "Montana",
        "Nebraska", "Nevada", "New Hampshire", "North Dakota", "Ohio", "Oregon",
        "Pennsylvania", "South Dakota", "Utah", "Vermont", "Washington", "Wisconsin",
        "Wyoming")
    illiteracy <- pqmat[ikeep, "Illiteracy", drop = FALSE]

    checkDuckDBMatrix(abs(income), abs(as.array(income)))
    checkDuckDBMatrix(sqrt(income), sqrt(as.array(income)))
    checkDuckDBMatrix(ceiling(income), ceiling(as.array(income)))
    checkDuckDBMatrix(floor(income), floor(as.array(income)))
    checkDuckDBMatrix(trunc(income), trunc(as.array(income)))

    expect_error(cummax(income))
    expect_error(cummin(income))
    expect_error(cumprod(income))
    expect_error(cumsum(income))

    checkDuckDBMatrix(log(income), log(as.array(income)))
    checkDuckDBMatrix(log10(income), log10(as.array(income)))
    checkDuckDBMatrix(log2(income), log2(as.array(income)))

    expect_error(log1p(income))

    checkDuckDBMatrix(acos(illiteracy), acos(as.array(illiteracy)))
    checkDuckDBMatrix(acosh(income), acosh(as.array(income)))
    checkDuckDBMatrix(asin(illiteracy), asin(as.array(illiteracy)))
    checkDuckDBMatrix(asinh(income), asinh(as.array(income)))
    checkDuckDBMatrix(atan(income), atan(as.array(income)))
    checkDuckDBMatrix(atanh(illiteracy), atanh(as.array(illiteracy)))

    checkDuckDBMatrix(exp(income), exp(as.array(income)))

    expect_error(expm1(income))

    checkDuckDBMatrix(cos(illiteracy), cos(as.array(illiteracy)))
    checkDuckDBMatrix(cosh(illiteracy), cosh(as.array(illiteracy)))

    expect_error(cospi(illiteracy))

    checkDuckDBMatrix(sin(illiteracy), sin(as.array(illiteracy)))
    checkDuckDBMatrix(sinh(illiteracy), sinh(as.array(illiteracy)))

    expect_error(sinpi(illiteracy))

    checkDuckDBMatrix(tan(illiteracy), tan(as.array(illiteracy)))
    checkDuckDBMatrix(tanh(illiteracy), tanh(as.array(illiteracy)))

    expect_error(tanpi(illiteracy))

    checkDuckDBMatrix(gamma(illiteracy), gamma(as.array(illiteracy)))
    checkDuckDBMatrix(lgamma(illiteracy), lgamma(as.array(illiteracy)))

    expect_error(digamma(illiteracy))
    expect_error(trigamma(illiteracy))
})
