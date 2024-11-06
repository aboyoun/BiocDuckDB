# Bioconductor-compatible Parquet objects

This package implements Bioconductor-friendly bindings to Parquet data so that they can be used inside standard objects like `DataFrame`s and `SummarizedExperiment`s.
Usage is pretty simple:

```r
tf <- tempfile()
arrow::write_parquet(cbind(model = rownames(mtcars), mtcars), tf)

library(ParquetDataFrame)
df <- ParquetDataFrame(tf, key = "model")
df
## ParquetDataFrame with 32 rows and 11 columns
##                               mpg             cyl            disp
##                   <DuckDBColumn> <DuckDBColumn> <DuckDBColumn>
## Mazda RX4                      21               6             160
## Mazda RX4 Wag                  21               6             160
## Datsun 710                   22.8               4             108
## Hornet 4 Drive               21.4               6             258
## Hornet Sportabout            18.7               8             360
## ...                           ...             ...             ...
## Lotus Europa                 30.4               4            95.1
## Ford Pantera L               15.8               8             351
## Ferrari Dino                 19.7               6             145
## Maserati Bora                  15               8             301
## Volvo 142E                   21.4               4             121
##                                hp            drat              wt
##                   <DuckDBColumn> <DuckDBColumn> <DuckDBColumn>
## Mazda RX4                     110             3.9            2.62
## Mazda RX4 Wag                 110             3.9           2.875
## Datsun 710                     93            3.85            2.32
## Hornet 4 Drive                110            3.08           3.215
## Hornet Sportabout             175            3.15            3.44
## ...                           ...             ...             ...
## Lotus Europa                  113            3.77           1.513
## Ford Pantera L                264            4.22            3.17
## Ferrari Dino                  175            3.62            2.77
## Maserati Bora                 335            3.54            3.57
## Volvo 142E                    109            4.11            2.78
##                              qsec              vs              am
##                   <DuckDBColumn> <DuckDBColumn> <DuckDBColumn>
## Mazda RX4                   16.46               0               1
## Mazda RX4 Wag               17.02               0               1
## Datsun 710                  18.61               1               1
## Hornet 4 Drive              19.44               1               0
## Hornet Sportabout           17.02               0               0
## ...                           ...             ...             ...
## Lotus Europa                 16.9               1               1
## Ford Pantera L               14.5               0               1
## Ferrari Dino                 15.5               0               1
## Maserati Bora                14.6               0               1
## Volvo 142E                   18.6               1               1
##                              gear            carb
##                   <DuckDBColumn> <DuckDBColumn>
## Mazda RX4                       4               4
## Mazda RX4 Wag                   4               4
## Datsun 710                      4               1
## Hornet 4 Drive                  3               1
## Hornet Sportabout               3               2
## ...                           ...             ...
## Lotus Europa                    5               2
## Ford Pantera L                  5               4
## Ferrari Dino                    5               6
## Maserati Bora                   5               8
## Volvo 142E                      4               2
```

This produces a file-backed `ParquetDataFrame`, consisting of file-backed `DuckDBColumn` objects.
These can be realized into memory via the usual `as.vector()` method.

```r
class(df$mpg)
## [1] "DuckDBColumn"
## attr(,"package")
## [1] "ParquetDataFrame"

as.vector(df$mpg)
##           Mazda RX4       Mazda RX4 Wag          Datsun 710      Hornet 4 Drive 
##                21.0                21.0                22.8                21.4 
##   Hornet Sportabout             Valiant          Duster 360           Merc 240D 
##                18.7                18.1                14.3                24.4 
##            Merc 230            Merc 280           Merc 280C          Merc 450SE 
##                22.8                19.2                17.8                16.4 
##          Merc 450SL         Merc 450SLC  Cadillac Fleetwood Lincoln Continental 
##                17.3                15.2                10.4                10.4 
##   Chrysler Imperial            Fiat 128         Honda Civic      Toyota Corolla 
##                14.7                32.4                30.4                33.9 
##       Toyota Corona    Dodge Challenger         AMC Javelin          Camaro Z28 
##                21.5                15.5                15.2                13.3 
##    Pontiac Firebird           Fiat X1-9       Porsche 914-2        Lotus Europa 
##                19.2                27.3                26.0                30.4 
##      Ford Pantera L        Ferrari Dino       Maserati Bora          Volvo 142E 
##                15.8                19.7                15.0                21.4 
```

We can now create a `SummarizedExperiment` consisting of a `ParquetDataFrame`, let's say in the `colData`:

```r
library(SummarizedExperiment)
se <- SummarizedExperiment(
    list(stuff = matrix(runif(nrow(df) * 10L), nrow = 10L)), # mocking up an assay
    colData = df
)
class(colData(se, withDimnames=FALSE))
## [1] "ParquetDataFrame"
## attr(,"package")
## [1] "ParquetDataFrame"
```

This behaves properly when we operate on the parent structure.

```r
se[, "Honda Civic"]
## class: SummarizedExperiment
## dim: 10 1
## metadata(0):
## assays(1): stuff
## rownames: NULL
## rowData names(0):
## colnames(1): Honda Civic
## colData names(11): mpg cyl ... gear carb

se[, se$cyl == 4L]
## class: SummarizedExperiment
## dim: 10 11
## metadata(0):
## assays(1): stuff
## rownames: NULL
## rowData names(0):
## colnames(11): Merc 240D Porsche 914-2 ... Datsun 710 Toyota Corona
## colData names(11): mpg cyl ... gear carb
```

Advanced users can also use the `dbconn` method to retrieve the query for the underlying
DuckDB table connection:

```r
dbconn(df)
## # Source:   SQL [?? x 12]
## # Database: DuckDB v1.1.2
##    model               mpg   cyl  disp    hp  drat    wt  qsec    vs    am  gear  carb
##    <chr>             <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl>
##  1 Mazda RX4          21       6  160    110  3.9   2.62  16.5     0     1     4     4
##  2 Mazda RX4 Wag      21       6  160    110  3.9   2.88  17.0     0     1     4     4
##  3 Datsun 710         22.8     4  108     93  3.85  2.32  18.6     1     1     4     1
##  4 Hornet 4 Drive     21.4     6  258    110  3.08  3.22  19.4     1     0     3     1
##  5 Hornet Sportabout  18.7     8  360    175  3.15  3.44  17.0     0     0     3     2
##  6 Valiant            18.1     6  225    105  2.76  3.46  20.2     1     0     3     1
##  7 Duster 360         14.3     8  360    245  3.21  3.57  15.8     0     0     3     4
##  8 Merc 240D          24.4     4  147.    62  3.69  3.19  20       1     0     4     2
##  9 Merc 230           22.8     4  141.    95  3.92  3.15  22.9     1     0     4     2
## 10 Merc 280           19.2     6  168.   123  3.92  3.44  18.3     1     0     4     4
## # ℹ more rows
## # ℹ Use `print(n = ...)` to see more rows
```
