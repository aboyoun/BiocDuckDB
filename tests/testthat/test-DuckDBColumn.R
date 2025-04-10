# Tests the basic functions of a DuckDBColumn.
# library(testthat); library(BiocDuckDB); source("setup.R"); source("test-DuckDBColumn.R")

test_that("head works for DuckDBColumn", {
    df <- DuckDBDataFrame(mtcars_parquet, datacols = colnames(mtcars), keycol = list(model = rownames(mtcars)))
    cyl <- df[["cyl"]]

    checkDuckDBColumn(head(cyl, 0), head(setNames(mtcars[["cyl"]], rownames(mtcars)), 0))
    checkDuckDBColumn(head(cyl, 20), head(setNames(mtcars[["cyl"]], rownames(mtcars)), 20))
})

test_that("tail works for DuckDBColumn", {
    df <- DuckDBDataFrame(mtcars_parquet, datacols = colnames(mtcars), keycol = list(model = rownames(mtcars)))
    cyl <- df[["cyl"]]

    checkDuckDBColumn(tail(cyl, 0), tail(setNames(mtcars[["cyl"]], rownames(mtcars)), 0))
    checkDuckDBColumn(tail(cyl, 20), tail(setNames(mtcars[["cyl"]], rownames(mtcars)), 20))
})

test_that("DuckDBColumn can be cast to a different type", {
    df <- DuckDBDataFrame(mtcars_parquet, datacols = colnames(mtcars), keycol = "model")
    cyl <- df[["cyl"]]

    checkDuckDBColumn(cyl, as.vector(cyl))

    type(cyl) <- "integer"
    expected <- as.vector(cyl)
    storage.mode(expected) <- "integer"
    checkDuckDBColumn(cyl, expected)
})

test_that("nonzero functions work for DuckDBColumn", {
    df <- DuckDBDataFrame(mtcars_parquet, datacols = colnames(mtcars), keycol = "model")
    am <- df[["am"]]
    checkDuckDBColumn(is_nonzero(am), is_nonzero(as.vector(am)))
    expect_equal(nzcount(am), nzcount(as.vector(am)))
})

test_that("unique works as expected for a DuckDBColumn", {
    df <- DuckDBDataFrame(mtcars_parquet, datacols = colnames(mtcars), keycol = "model")
    cyl <- df[["cyl"]]
    expected <- unique(as.vector(cyl))
    names(expected) <- NULL
    checkDuckDBColumn(unique(cyl), expected)
})

test_that("Arith methods work as expected for a DuckDBColumn", {
    df <- DuckDBDataFrame(mtcars_parquet, datacols = colnames(mtcars), keycol = "model")
    mpg <- df[["mpg"]]
    disp <- df[["disp"]]

    checkDuckDBColumn(mpg + disp, as.vector(mpg) + as.vector(disp))
    checkDuckDBColumn(mpg - 1L, as.vector(mpg) - 1L)
    checkDuckDBColumn(mpg * 3.14, as.vector(mpg) * 3.14)
    checkDuckDBColumn(1L / mpg, 1L / as.vector(mpg))
    checkDuckDBColumn(3.14 ^ mpg, 3.14 ^ as.vector(mpg))
    checkDuckDBColumn(mpg %% disp, as.vector(mpg) %% as.vector(disp))
    checkDuckDBColumn(mpg %/% 3.14, as.vector(mpg) %/% 3.14)
})

test_that("Compare methods work as expected for a DuckDBColumn", {
    df <- DuckDBDataFrame(mtcars_parquet, datacols = colnames(mtcars), keycol = "model")
    vs <- df[["vs"]]
    am <- df[["am"]]

    checkDuckDBColumn(vs == am, as.vector(vs) == as.vector(am))
    checkDuckDBColumn(vs > 1L, as.vector(vs) > 1L)
    checkDuckDBColumn(vs < 1L, as.vector(vs) < 1L)
    checkDuckDBColumn(1L != vs, 1L != as.vector(vs))
    checkDuckDBColumn(1 <= vs, 1 <= as.vector(vs))
    checkDuckDBColumn(vs >= am, as.vector(vs) >= as.vector(am))
})

test_that("Math methods work as expected for a DuckDBColumn", {
    df <- DuckDBDataFrame(mtcars_parquet, datacols = colnames(mtcars), keycol = "model")
    mpg <- df[["mpg"]]

    checkDuckDBColumn(abs(mpg), abs(as.vector(mpg)))
    checkDuckDBColumn(sqrt(mpg), sqrt(as.vector(mpg)))
    checkDuckDBColumn(ceiling(mpg), ceiling(as.vector(mpg)))
    checkDuckDBColumn(floor(mpg), floor(as.vector(mpg)))
    checkDuckDBColumn(trunc(mpg), trunc(as.vector(mpg)))

    expect_error(cummax(mpg))
    expect_error(cummin(mpg))
    expect_error(cumprod(mpg))
    expect_error(cumsum(mpg))

    checkDuckDBColumn(log(mpg), log(as.vector(mpg)))
    checkDuckDBColumn(log10(mpg), log10(as.vector(mpg)))
    checkDuckDBColumn(log2(mpg), log2(as.vector(mpg)))

    expect_error(log1p(mpg))
})

test_that("%in% works as expected for a DuckDBColumn", {
    df <- DuckDBDataFrame(mtcars_parquet, datacols = colnames(mtcars), keycol = "model")
    carb <- df[["carb"]]

    checkDuckDBColumn(carb %in% c(2, 4, 8), setNames(as.vector(carb) %in% c(2, 4, 8), names(carb)))
})

test_that("Special numeric functions work as expected for a DuckDBColumn", {
    df <- DuckDBDataFrame(special_path, datacols = "x", keycol = list(id = letters[1:4]))
    x <- df[["x"]]

    checkDuckDBColumn(is.finite(x), is.finite(as.vector(x)))
    checkDuckDBColumn(is.infinite(x), is.infinite(as.vector(x)))
    checkDuckDBColumn(is.nan(x), is.nan(as.vector(x)))
})

test_that("Special spatial functions work as expected for a DuckDBColumn", {
    df <- DuckDBDataFrame(spatial_path)
    df <- df[which(!is.na(spatial_wkt)),]

    x <- df[["geometry"]]
    type <- df[["type"]]
    sfc <- st_as_sfc(spatial_wkt[!is.na(spatial_wkt)])

    checkDuckDBColumn(st_area(x), st_area(sfc))
    checkDuckDBColumn(st_as_binary(x, hex = TRUE), st_as_binary(sfc, hex = TRUE))
    checkDuckDBColumn(st_as_text(x), st_as_text(sfc))
    checkDuckDBColumn(st_as_text(st_boundary(x)), st_as_text(st_boundary(sfc)))
    checkDuckDBColumn(st_as_text(st_centroid(x)), st_as_text(st_centroid(sfc)))
    checkDuckDBColumn(st_as_text(st_convex_hull(x)), st_as_text(st_convex_hull(sfc)))
    checkDuckDBColumn(st_as_text(st_exterior_ring(x)),
                      c(rep(NA, 15), "LINESTRING (30 10, 40 40, 20 40, 10 20, 30 10)",
                        "LINESTRING (35 10, 45 45, 15 40, 10 20, 35 10)", "LINESTRING EMPTY"))
    checkDuckDBColumn(st_is_valid(x), st_is_valid(sfc))
    checkDuckDBColumn(st_as_text(st_line_merge(df[df$type == "multilinestring", "geometry"])),
                      st_as_text(st_line_merge(sfc[3:5])))
    checkDuckDBColumn(st_as_text(st_line_merge(df[df$type == "multilinestring", "geometry"], directed = TRUE)),
                      st_as_text(st_line_merge(sfc[3:5], directed = TRUE)))
    checkDuckDBColumn(st_as_text(st_make_valid(x)), st_as_text(st_make_valid(sfc)))
    checkDuckDBColumn(st_as_text(st_normalize(x)), st_as_text(st_normalize(sfc)))
    checkDuckDBColumn(st_as_text(st_point_on_surface(x)), st_as_text(st_point_on_surface(sfc)))
    checkDuckDBColumn(st_as_text(st_reverse(x)), st_as_text(st_reverse(sfc)))
})

test_that("Summary methods work as expected for a DuckDBColumn", {
    df <- DuckDBDataFrame(mtcars_parquet, datacols = colnames(mtcars), keycol = "model")
    mpg <- df[["mpg"]]
    am <- df[["am"]]
    expect_identical(max(mpg), max(as.vector(mpg)))
    expect_identical(min(mpg), min(as.vector(mpg)))
    expect_identical(range(mpg), range(as.vector(mpg)))
    expect_equal(prod(mpg), prod(as.vector(mpg)))
    expect_equal(sum(mpg), sum(as.vector(mpg)))
    expect_identical(any(am == 0L), any(as.vector(am) == 0L))
    expect_identical(all(am == 0L), all(as.vector(am) == 0L))
})

test_that("Other aggregate methods work as expected for a DuckDBColumn", {
    df <- DuckDBDataFrame(mtcars_parquet, datacols = colnames(mtcars), keycol = "model")
    mpg <- df[["mpg"]]
    expect_equal(mean(mpg), mean(as.vector(mpg)))
    expect_equal(median(mpg), median(as.vector(mpg)))
    expect_equal(var(mpg), var(as.vector(mpg)))
    expect_equal(sd(mpg), sd(as.vector(mpg)))
    expect_equal(mad(mpg), mad(as.vector(mpg)))
    expect_equal(mad(mpg, constant = 1), mad(as.vector(mpg), constant = 1))

    expect_equal(quantile(mpg), quantile(as.vector(mpg)))
    expect_equal(quantile(mpg, probs = seq(0, 1, by = 0.05)), quantile(as.vector(mpg), probs = seq(0, 1, by = 0.05)))
    expect_equal(quantile(mpg, names = FALSE), quantile(as.vector(mpg), names = FALSE))
    expect_equal(quantile(mpg, type = 1), quantile(as.vector(mpg), type = 1))

    expect_equal(IQR(mpg), IQR(as.vector(mpg)))
    expect_equal(IQR(mpg, type = 1), IQR(as.vector(mpg), type = 1))

    gear <- df[["gear"]]
    carb <- df[["carb"]]
    expect_equal(table(gear, carb), table(gear = as.vector(gear), carb = as.vector(carb)))
})
