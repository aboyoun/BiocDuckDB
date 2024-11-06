#' Key Names and Key Count
#'
#' Get the keycols names, keycols dimension names, or keycols count of an object.
#'
#' @param x An object to get the keycols related information.
#' @param value A character vector of keycols dimension names.
#'
#' @author Patrick Aboyoun
#'
#' @examples
#' keynames
#' showMethods("keynames")
#'
#' keydimnames
#' showMethods("keydimnames")
#'
#' nkey
#' showMethods("nkey")
#'
#' @aliases
#' keynames
#' keydimnames
#' keydimnames<-
#' nkey
#' nkeydim
#'
#' keynames,DuckDBTable-method
#' keydimnames,DuckDBTable-method
#' keydimnames<-,DuckDBTable-method
#' nkey,DuckDBTable-method
#' nkeydim,DuckDBTable-method
#'
#' @name keynames
NULL

#'
#' @export
#' @rdname keynames
setGeneric("keynames", function(x) standardGeneric("keynames"))

#' @export
#' @rdname keynames
setGeneric("keydimnames", function(x, value) standardGeneric("keydimnames"))

#' @export
#' @rdname keynames
setGeneric("keydimnames<-", function(x, value) standardGeneric("keydimnames<-"))

#' @export
#' @rdname keynames
setGeneric("nkey", function(x) standardGeneric("nkey"))

#' @export
#' @rdname keynames
setGeneric("nkeydim", function(x) standardGeneric("nkeydim"))
