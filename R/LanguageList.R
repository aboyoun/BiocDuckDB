#' Lists of Language Objects
#'
#' @description
#' An extension of \code{\linkS4class{SimpleList}} that holds \code{language} objects.
#'
#' @param \dots language objects, possibly named.
#'
#' @examples
#' getClassDef("language")
#' LanguageList(jenny = quote(867-5309))
#' LanguageList(x = quote(x), sqrtx = quote(sqrt(x)), logx = quote(log(x)))
#'
#' @name LanguageList
#' @aliases LanguageList-class
NULL

#' @importClassesFrom S4Vectors SimpleList
#' @export
setClass("LanguageList", contains = "SimpleList", prototype = prototype(elementType = "language"))

#' @export
#' @rdname LanguageList
LanguageList <- function(...) {
    args <- list(...)
    if (length(args) == 1L && inherits(args[[1L]], "list")) {
        args <- args[[1L]]
    }
    if (!all(vapply(args, is.language, logical(1L), USE.NAMES = FALSE))) {
        stop("All elements must be language objects.")
    }
    new2("LanguageList", listData = args, check = FALSE)
}
