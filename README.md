# Bioconductor-compatible DuckDB objects

## Overview

BiocDuckDB leverages the power of DuckDB to enhance Bioconductor's relational
data structures, such as `SummarizedExperiment` and `MultiAssayExperiment`. It
introduces specialized classes to efficiently manage large datasets:

- `DuckDBMatrix`: Optimizes assay data storage and retrieval.
- `DuckDBGRanges` and `DuckDBGRangesList`: Facilitate handling of genomic
  ranges.
- `DuckDBDataFrame`: Manages row and column metadata seamlessly.

These classes serve to integrate DuckDB's high-performance database capabilities
directly within Bioconductor workflows, offering a robust solution for
large-scale data analysis.

## Usage

Here is an illustration using the `airway` sample dataset:

```r
library(BiocDuckDB)
library(SummarizedExperiment)

data(airway, package = "airway")

airway_rowranges_td <- tempfile(pattern = "rowranges_")
arrow::write_dataset(as.data.frame(rowRanges(airway)), airway_rowranges_td, format = "parquet")

airway_rowdata_td <- tempfile(pattern = "rowdata_")
arrow::write_dataset(as.data.frame(rowData(airway)), airway_rowdata_td, format = "parquet")

airway_coldata_td <- tempfile(pattern = "coldata_")
arrow::write_dataset(as.data.frame(colData(airway)), airway_coldata_td, format = "parquet")

airway_counts_td <- tempfile(pattern = "counts_")
writeCoordArray(assays(airway)[["counts"]], airway_counts_td, keycols = c("gene_id", "run"))

rranges <- DuckDBGRanges(airway_rowranges_td, seqnames = "seqnames", start = "start", end = "end",
                         strand = "strand", mcols = c("exon_id", "exon_name", "group_name"),
                         seqinfo = seqinfo(airway))

rdata <- DuckDBDataFrame(airway_rowdata_td, keycol = "gene_id")
rdata <- rdata[sort(rownames(rdata)), ]

rranges <- split(rranges, rranges$group_name)
mcols(rranges) <- rdata

cdata <- DuckDBDataFrame(airway_coldata_td, keycol = "Run")
cdata <- cdata[sort(rownames(cdata)), ]

counts <- DuckDBMatrix(airway_counts_td, row = "gene_id", col = "run", datacol = "value")
counts <- counts[rownames(rdata), sort(rownames(cdata))]

airway_ddb <- SummarizedExperiment(assays = SimpleList(counts = counts),
                                   rowRanges = rranges, colData = cdata,
                                   metadata = metadata(airway))
```

This produces a `RangedSummarizedExperiment` where the genomic row ranges are a
`DuckDBGRangesList`:

```r
rowRanges(airway_ddb)
# DuckDBGRangesList object of length 63677:
# $ENSG00000000003
# DuckDBGRanges object with 17 ranges and 3 metadata columns:
#        seqnames     start       end     width      strand |   exon_id
#     <character> <integer> <integer> <integer> <character> | <integer>
# 1             X  99883667  99884983      1317           - |    667145
# 2             X  99885756  99885863       108           - |    667146
# 3             X  99887482  99887565        84           - |    667147
# 4             X  99887538  99887565        28           - |    667148
# 5             X  99888402  99888536       135           - |    667149
# 6             X  99888402  99888536       135           - |    667150
# 7             X  99888439  99888536        98           - |    667151
# 8             X  99888928  99889026        99           - |    667153
# 9             X  99888928  99889026        99           - |    667152
# 10            X  99890175  99890249        75           - |    667154
# ...         ...       ...       ...       ...         ... .       ...
#           exon_name      group_name
#         <character>     <character>
# 1   ENSE00001459322 ENSG00000000003
# 2   ENSE00000868868 ENSG00000000003
# 3   ENSE00000401072 ENSG00000000003
# 4   ENSE00001849132 ENSG00000000003
# 5   ENSE00003554016 ENSG00000000003
# 6   ENSE00003658801 ENSG00000000003
# 7   ENSE00001895484 ENSG00000000003
# 8   ENSE00003658810 ENSG00000000003
# 9   ENSE00003552498 ENSG00000000003
# 10  ENSE00003654571 ENSG00000000003
# ...             ...             ...
# -------
# seqinfo: 722 sequences (1 circular) from an unspecified genome
# 
# ...
# <63676 more elements>
```

the row and column metadata are `DuckDBDataFrame`s:

```r
rowData(airway_ddb)
# DuckDBDataFrame with 63677 rows and 10 columns
#                         gene_id     gene_name  entrezid   gene_biotype
#                     <character>   <character> <integer>    <character>
# ENSG00000000003 ENSG00000000003        TSPAN6        NA protein_coding
# ENSG00000000005 ENSG00000000005          TNMD        NA protein_coding
# ENSG00000000419 ENSG00000000419          DPM1        NA protein_coding
# ENSG00000000457 ENSG00000000457         SCYL3        NA protein_coding
# ENSG00000000460 ENSG00000000460      C1orf112        NA protein_coding
# ...                         ...           ...       ...            ...
# ENSG00000273489 ENSG00000273489 RP11-180C16.1        NA      antisense
# ENSG00000273490 ENSG00000273490        TSEN34        NA protein_coding
# ENSG00000273491 ENSG00000273491  RP11-138A9.2        NA        lincRNA
# ENSG00000273492 ENSG00000273492    AP000230.1        NA        lincRNA
# ENSG00000273493 ENSG00000273493  RP11-80H18.4        NA        lincRNA
#                 gene_seq_start gene_seq_end              seq_name seq_strand
#                      <integer>    <integer>           <character>  <integer>
# ENSG00000000003       99883667     99894988                     X         -1
# ENSG00000000005       99839799     99854882                     X          1
# ENSG00000000419       49551404     49575092                    20         -1
# ENSG00000000457      169818772    169863408                     1         -1
# ENSG00000000460      169631245    169823221                     1          1
# ...                        ...          ...                   ...        ...
# ENSG00000273489      131178723    131182453                     7         -1
# ENSG00000273490       54693789     54697585 HSCHR19LRC_LRC_J_CTG1          1
# ENSG00000273491      130600118    130603315          HG1308_PATCH          1
# ENSG00000273492       27543189     27589700                    21          1
# ENSG00000273493       58315692     58315845                     3          1
#                 seq_coord_system        symbol
#                        <integer>   <character>
# ENSG00000000003               NA        TSPAN6
# ENSG00000000005               NA          TNMD
# ENSG00000000419               NA          DPM1
# ENSG00000000457               NA         SCYL3
# ENSG00000000460               NA      C1orf112
# ...                          ...           ...
# ENSG00000273489               NA RP11-180C16.1
# ENSG00000273490               NA        TSEN34
# ENSG00000273491               NA  RP11-138A9.2
# ENSG00000273492               NA    AP000230.1
# ENSG00000273493               NA  RP11-80H18.4

colData(airway_ddb)
# DuckDBDataFrame with 8 rows and 9 columns
#             SampleName        cell         dex       albut         Run avgLength
#            <character> <character> <character> <character> <character> <integer>
# SRR1039508  GSM1275862      N61311       untrt       untrt  SRR1039508       126
# SRR1039509  GSM1275863      N61311         trt       untrt  SRR1039509       126
# SRR1039512  GSM1275866     N052611       untrt       untrt  SRR1039512       126
# SRR1039513  GSM1275867     N052611         trt       untrt  SRR1039513        87
# SRR1039516  GSM1275870     N080611       untrt       untrt  SRR1039516       120
# SRR1039517  GSM1275871     N080611         trt       untrt  SRR1039517       126
# SRR1039520  GSM1275874     N061011       untrt       untrt  SRR1039520       101
# SRR1039521  GSM1275875     N061011         trt       untrt  SRR1039521        98
#             Experiment      Sample    BioSample
#            <character> <character>  <character>
# SRR1039508   SRX384345   SRS508568 SAMN02422669
# SRR1039509   SRX384346   SRS508567 SAMN02422675
# SRR1039512   SRX384349   SRS508571 SAMN02422678
# SRR1039513   SRX384350   SRS508572 SAMN02422670
# SRR1039516   SRX384353   SRS508575 SAMN02422682
# SRR1039517   SRX384354   SRS508576 SAMN02422673
# SRR1039520   SRX384357   SRS508579 SAMN02422683
# SRR1039521   SRX384358   SRS508580 SAMN02422677
```

and the counts assay is a `DuckDBMatrix`:

```r
assays(airway_ddb)[["counts"]]
# <63677 x 8> sparse DuckDBMatrix object of type "integer":
#                 SRR1039508 SRR1039509 ... SRR1039520 SRR1039521
# ENSG00000000003        679        448   .        770        572
# ENSG00000000005          0          0   .          0          0
# ENSG00000000419        467        515   .        417        508
# ENSG00000000457        260        211   .        233        229
# ENSG00000000460         60         55   .         76         60
#             ...          .          .   .          .          .
# ENSG00000273489          0          0   .          0          0
# ENSG00000273490          0          0   .          0          0
# ENSG00000273491          0          0   .          0          0
# ENSG00000273492          0          0   .          0          0
# ENSG00000273493          0          0   .          0          0
```

## Contributing

Contributions are welcome. Please report any issues or feature requests through
the GitHub issue tracker. Follow Bioconductor guidelines for coding and
documentation.

## License

BiocDuckDB is licensed under the MIT License. See the LICENSE file for more
details.

## Acknowledgements

Special thanks to the Bioconductor project for the support and infrastructure.
