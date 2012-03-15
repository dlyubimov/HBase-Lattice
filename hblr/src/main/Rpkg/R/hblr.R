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
.hbl.init <- function (pkgname,libname=NULL,pkgInit=F) {
	
	library(rJava)
	library(ecor)
	
	if ( length(pkgname) == 0 ) pkgname <- "hblr"
	
	hbl <- list()
	hbl$options <- list()
	hbl$consts <- list()
	hbl$consts$HBASE_CONNECTION_PER_CONFIG <- "hbase.connection.per.config"

	hbl$HOME <- system.file(package=pkgname,lib.loc=libname)
	hbl$cp <- list.files(system.file("java", package=pkgname), full.names=T,
			pattern="\\.jar$")

	hadoopcp <- ecor.hadoopClassPath()
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
			
	.jpackage(pkgname, morePaths = c(hadoopcp, hbasecp, pigcp), lib.loc = libname)	
	
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

#' 
#' Use to re-prepare query obtained thru hbl.hblquery()
#' 
#' d
#' 
#' @param x 
#' 
hbl.prepare <- function (x, ...) UseMethod("prepare")

#'
#' set parameters (there's some todo here probably still)
#' 
hbl.setParameter <- function (x, ...) UseMethod("setParameter")

#'
#' execute prepared HBL query
#' @param x the prepared query object 
#' 

hbl.execute <- function (x, ...) UseMethod ("execute")

#' prepared query class constructor
#' 
#' d
#' 
#' @param qstr the query string to prepare.
#' @S3class hblquery, if specified, then query also will be prepared.
#' @return new hblquery object, prepared.
#'  
hbl.hblquery <- function ( qstr = NULL ) {
	
	q <- list() 
	class(q) <- "hblquery"
	q$queryClient <- hbl$queryClient
	q$q <- q$queryClient$createPreparedQuery()
	if ( length(qstr) > 0 ) {
		prepare.hblquery(q, qstr)
	}
	q
}

#' prepare hbl query 
#' 
#' d
#' 
#' @method prepare hblquery
#' @param q query object
#' @param qstr query string 
prepare.hblquery <- function (q, qstr) {
	
	qstr <- as.character(qstr)
	q$q$prepare(qstr)
	
	#debug
	#cat ("query prepared:", qstr)
	
	q
} 

setParameter.hblquery <- function (q, paramIndex, value ) {
	# rely on rJava conversions at this point
	q$q$setHblParameter(paramIndex,value);
	q
}

execute.hblquery <- function (q ) {
	rs <- q$q$execute() 
  on.exit(rs$close(), add=T)
	aliases <- sapply ( rs$getAliases(), function(alias) as.character(alias$toString()))
	
	if (! rs$hasNext() ) {
		#todo: is there a more efficient way of doing this?
		r <- data.frame(stringsAsFactors=F)
		for (alias in aliases) r[[alias]] <- character(0)
		return(r)
	}
	
	r <- NULL
	rs$"next"() 
	datarow <- .convertRS(aliases,rs$current())
	r <- data.frame(datarow, stringsAsFactors=F)

	while ( rs$hasNext() ) {
		rs$"next"()
		datarow <- .convertRS(aliases,rs$current())
		r<- rbind(r,datarow)
	}
	r 
}

# convert a result set row to a list of values 
# denoted by 
.convertRS <- function (aliases, hblrow ) {
	sapply(aliases, function(alias) { 
				a <- hblrow$getObject(alias)
				if ( mode(a)=='raw') 
					# handling hex dimension values, byte arrays
					a<- paste(format(as.hexmode(as.integer(a)),width=2,upper.case=T),collapse="")
				if ( mode(a) =='S4')
					a<- a$toString()
				a
		},
		simplify=F)
}

##################################
# HblAdmin                       #
##################################

#generic admin functions

#' generic drop function
#' 
#' drop (hbl) cube 
#' 
#' @param x cube to use 
hbl.dropCube <- function (x,...) UseMethod("dropCube")
hbl.deployCube <- function (x,...) UseMethod("deployCube")
hbl.saveModel <- function (x,...) UseMethod("saveModel")

#' produce admin object from yaml. 
#' 
#' @S3class hbladmin
#' @return hbladmin object
hbl.admin.fromYaml <- function (model.yaml) {
	
  	admin <- list()
  	class(admin) <- "hbladmin"
  	yaml <- paste(as.character(model.yaml),collapse='\n')
  
  	bytes <- new (J("java.lang.String"),yaml)$getBytes('utf-8')

  	resource <- new(J("org.springframework.core.io.ByteArrayResource"), 
    	.jarray(bytes))

  	admin$adm <- new(J("com.inadco.hbl.client.HblAdmin"),resource)
  
  	admin
} 

hbl.admin.fromYamlFile <- function (model.file.name) {
	
  	f <- file(model.file.name,'r')
   	s <- readLines(f)
   	close(f)
   	hbl.admin.fromYaml(s)
}

hbl.admin.fromCube <- function (cube.name ) {
	
  	admin <- list()
  	class(admin) <- "hbladmin"
  	admin$adm <- new (J("com.inadco.hbl.client.HblAdmin"), 
      	as.character(cube.name),
      	hbl$conf
      	)
  	admin
}

dropCube.hbladmin <- function(admin) {
  	admin$dropCube(hbl$conf)
} 

deployCube.hbladmin <- function(admin) { 
  	admin$deployCube(hbl$conf)
}

saveModel.hbladmin <- function(admin) {
  	admin$saveModel(hbl$conf)
}

###################################
# incremental compilation         # 
###################################



