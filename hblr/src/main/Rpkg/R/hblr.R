#' R wrapper for running hbl queries etc. using rJava
#' 
#' @docType package
#' @name hblr
#' @import ecor
#' @import rJava
#' @exportPattern "^hbl\\."
#'  
#'
NULL 


.onLoad <- function (libname=NULL,pkgname=NULL) .hbl.init(libname,pkgname, pkgInit=T)
.onUnload <- function(libpath) rm(hbl) 
##########################
# generic initialization #
##########################

# we assume that hadoop, hbase and HBL 
# homes are required and PIG_HOME is 
# optional (but if pig is not installed 
# then compiler functions will not work
# in this session).
.hbl.init <- function (libname=NULL,pkgname=NULL,pkgInit=F) {
	
	library(rJava)
	library(ecor)
	
	if ( length(pkgname) == 0 ) pkgname <- "hblr"
	
	hbl <- new.env()
	hbl$options <- list()
	hbl$consts <- list()
	hbl$consts$HBASE_CONNECTION_PER_CONFIG <- "hbase.connection.per.config"

	hbl$HOME <- system.file(package=pkgname,lib.loc=libname)
	
	cp <- list.files(system.file("java",package=pkgname,lib.loc=libname),
			full.names=T, pattern ="\\.jar$")
	hbl$cp <- cp
	
	hbasecp <- ecor.hBaseClassPath()
	pigcp <- NULL
	hbl$pig <- F

	tryCatch({
				pigcp <- ecor.pigClassPath()
				hbl$pig <- T
			}, 
			error = function (e) {
				cat("Warning: pig installation was not found, set PIG_HOME. some functionality will not be available.\n",
						as.character(e))
			}
			)
			
	.jpackage(pkgname, morePaths = c(hbasecp, pigcp), lib.loc = libname)	
	
	hbl <<- hbl
	
	.hbl.createHblClient()
}

.hbl.createHblClient <- function () {
	
	hbl$jconf <- J("org.apache.hadoop.hbase.HBaseConfiguration")$create(ecor$jconf)
	
	# cdh3u3 or later requred
	hbl$jconf$setBoolean(hbl$consts$HBASE_CONNECTION_PER_CONFIG,F)
	
	hbl$queryClient <- new(J("com.inadco.hbl.client.HblQueryClient"),hbl$jconf)
	
}

.hbl.checkPig <- function() { 
	if ( !hbl$pig  )
		stop("pig access is not initialized in this session (have you set PIG_HOME?)")
}

#############################################
# hbl query methods for R class "hblquery"  #
#############################################

#' prepared query class constructor
#' 
#' d
#' 
#' @param qstr the query string to prepare.
#' @method initialize HblQuery
#' @return none
#'  
initialize.HblQuery <- function ( qstr = NULL ) {
	
	queryClient <<- hbl$queryClient
	q <<- queryClient$createPreparedQuery()
	if ( length(qstr) > 0 ) {
		prepare(qstr)
	}
	
}

#' @title
#' prepare hbl query 
#' 
#' @description 
#' (Re-)prepare HBL query by supplying a new query string.
#' 
#' @details
#' this method parses query into AST tree so that parsing is not 
#' repeated again every time prepared query executes. 
#' 
#' @method prepare HblQuery
#' @param qstr query string 
prepare.HblQuery <- function (qstr) {
	
	qstr <- as.character(qstr)
	if ( length(qstr)>1 ) 
		qstr <- paste(qstr,collapse="")
	q$prepare(qstr)
} 

#' @title 
#' setParameter.
#' 
#' @description
#' set hbl query parameter
#' 
#' @details 
#' hbl query parameters are denoted by '?' symbol in the query string. 
#' Substitution with actual values should happen after preparing query 
#' but before execution. 
#' 
#' @param paramIndex ordinal index of the parameter in the query (0-based). 
#' This is identical to JDBC's interpretation of positional query parameters.
#' @param value the actual value for the parameter.
#'  
setParameter.HblQuery <- function (paramIndex, value ) {
	
	clazz <- as.character(class(value))
	if ( any(clazz == "POSIXct") ) { 
		#convert R time to long 
		value <- .jnew("java.lang.Long", .jlong(as.numeric(value)*1000))
	} else if (clazz =="raw") {
		value <- .jarray(value)
	} else if ( clazz=="character") {
		value <- .jnew("java.lang.String", value)
	} else if ( class=="numeric") {
		value <- .jnew("java.lang.Double", value)
	} else if ( class=="integer") {
		value <- .jnew("java.lang.Integer", value)
	} else if ( class=="jobjRef") { 
	} else {
		stop(sprintf("Don't know how to convert parameter class %s",clazz))
	}
	
	# rely on rJava conversions at this point..
	# which is,heh, slow... 
	q$setHblParameter(as.integer(paramIndex),.jcast(value,"java.lang.Object"))
}

#' @title 
#' execute
#' 
#' @description 
#' execute hbl query
#' 
#' @details 
#' Once hbl query is prepared and all parameters are set with \link{setParameter} method, 
#' the query can be executed.\cr\cr
#' 
#' The query can be executed multiple times (and with different parameters if desired) 
#' without having to re-prepare it.
#' 
#' @method execute HblQuery
#' @return data frame corresponding to query results. Data frame names correspond to the 
#' aliases used in the query. 
#' 
execute.HblQuery <- function ( ) {
	rs <- q$execute() 
    on.exit(rs$close(), add=T)
	aliases <- sapply ( rs$getAliases(), function(alias) as.character(alias$toString()))
	
	
	r <- data.frame(stringsAsFactors=F)
	
	nextRow <- 0
	while ( .jcall(rs,"Z","hasNext", simplify=T) ) {
		nextRow <- nextRow+1 
		.jcall(rs,"V","next")
		hblrow <- .jcall(rs,"Ljava/lang/Object;","current")
		for (alias in aliases) {
			a <- .jcall(hblrow,"Ljava/lang/Object;","getObject", alias, evalArray=T, simplify = T, use.true.class=T)
			if ( mode(a)=='raw') { 
				# handling hex dimension values, byte arrays
				a<- paste(format(as.hexmode(as.integer(a)),width=2,upper.case=T),collapse="")
				
			} else if ( as.character(class(a)) =='jobjRef') {
				if ( a%instanceof%"java.lang.Number") {
					a<-.jcall(a,"D","doubleValue",simplify=T)	
				} else {
					a<- .jcall(a,"Ljava/lang/String;","toString",simplify = T)
				}
			}
			r[nextRow,alias] <- a
		}
	}
	if ( nextRow == 0 ) { 
			#todo: is there a more efficient way of doing this?
			r <- data.frame(stringsAsFactors=F)
			for (alias in aliases) r[[alias]] <- character(0)
	}
	r 
}



#' HblQuery class 
#' 
#' R5 class implementing HBL query object.
hbl.HblQuery <- setRefClass("HblQuery",
		fields=list(
				q="jobjRef",
				queryClient="jobjRef"
		),
		methods=list(
				initialize = initialize.HblQuery,
				prepare = prepare.HblQuery,
				setParameter = setParameter.HblQuery,
				execute = execute.HblQuery
		)
)


##################################
# HblAdmin                       #
##################################

#'@title 
#' Initialize HblAdmin object 
#' 
#' @description
#' 
#' Initialize an HblAdmin object with cube model in one of several ways 
#' presented. 
#'  
#' @details
#' 
#' \strong{Only one of} \code{model.yaml}, \code{model.file.name} or 
#' \code{cube.name} should be given. \cr\cr
#' 
#' If \code{model.yaml} is given, then it is coerced to a single character 
#' string (if vector has more than one entry, they are separated by \\n) 
#' and that string is parsed to build & instantiate cube model.\cr\cr
#' 
#' If \code{model.file.name} is given, then model is loaded from that file 
#' and then initialization proceeds the same as per above paragraph.\cr\cr
#' 
#' if \code{cube.name} is given (as a single-item character vector) 
#' then the model is loaded from an HBL 'system' table in HBase and the cube name 
#' is used to locate the model in the system table.\cr\cr
#' 
#' @param model.yaml character vector containing hbl model's description in yaml.
#' @param model.file.name file name containing hbl model's description in yaml.
#' @param cube.name cube name in HBL HBase 'system' table to load from.
#'   
#' 
initialize.HblAdmin <- function (model.yaml=NULL, model.file.name=NULL, cube.name = NULL ) {
	
	if ( length(model.yaml) > 0 ) {
		
		yaml <- paste(as.character(model.yaml),collapse='\n')
		
		bytes <- new (J("java.lang.String"),yaml)$getBytes('utf-8')
		
		resource <- new(J("org.springframework.core.io.ByteArrayResource"), 
				.jarray(bytes))
		
		adm <<- new(J("com.inadco.hbl.client.HblAdmin"),resource)
		
	} else if ( length( model.file.name)==1 ) {
		
		f <- file(model.file.name,'r')
		tryCatch(
			s <- readLines(f),
			finally=close(f))
		.self$initialize(model.yaml=s)
		
	} else if ( length(cube.name)==1 ) {
		
		adm <<- new (J("com.inadco.hbl.client.HblAdmin"), 
				as.character(cube.name),
				hbl$jconf)

	} else {
		stop("invalid arguments to initialize hblAdmin instance.")
	}

}

dropCube.HblAdmin <- function() {
  	adm$dropCube(hbl$jconf)
} 

deployCube.HblAdmin <- function() { 
  	adm$deployCube(hbl$jconf)
}

saveModel.HblAdmin <- function() {
  	adm$saveModel(hbl$jconf)
}

hbl.HblAdmin <- setRefClass("HblAdmin",
		fields=list(
				adm="jobjRef"),
		methods=list(
				initialize=initialize.HblAdmin,
				dropCube=dropCube.HblAdmin,
				deployCube=deployCube.HblAdmin,
				saveModel=saveModel.HblAdmin
		))

###################################
# incremental compilation         # 
###################################

#To be contd...

