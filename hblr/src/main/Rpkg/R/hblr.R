# TODO: Add comment
# 
# Author: dmitriy
###############################################################################


library("rJava");
hbl <- list()

hbl.init <- function () {
	
	hbl <- list()
	hbl$options <- list()
	hbl$consts <- list()
	hbl$consts$HBASE_CONNECTION_PER_CONFIG <- "hbase.connection.per.config"
	
    hbl$options$HBL_HOME <- Sys.getenv("HBL_HOME");
	hbl$options$HADOOP_HOME <- Sys.getenv("HADOOP_HOME");
	hbl$options$HBASE_HOME <- Sys.getenv("HBASE_HOME");
	
	if ( nchar(hbl$options$HADOOP_HOME) ==0 )
		stop ("HADOOP_HOME not set");
	
	if ( nchar(hbl$options$HBL_HOME) == 0 ) 
		stop("HBL_HOME not set");
	
	if ( nchar(hbl$options$HBASE_HOME)==0 ) 
		stop("HBASE_HOME not set");
	
	
	cp1 <- list.files(
			paste( hbl$options$HADOOP_HOME, "lib", sep="/" ), 
			full.names = T,
			pattern="\\.jar$")
	
	core <- list.files (
			hbl$options$HADOOP_HOME,
			full.names=T,
			pattern=".*core.*\\.jar"
			)
	
	cp2 <- list.files (
			paste(hbl$options$HBL_HOME, "lib", sep="/"),
			full.names = T,
			pattern="\\.jar$"
			)
		
	cp3 <- character(0)
	
	for ( assemblyDir in list.files (
			paste(hbl$options$HBL_HOME,"hbl/target",sep="/"),
			full.names=T,
			pattern="hbl-.*-dist$",
			include.dirs=T
			)) {  
				
		cp3 <- c(cp3, list.files(
						paste(assemblyDir, "lib", sep="/"),
						full.names=T,
						pattern="\\.jar$"
						))
	}
	
	
	hb_core <- list.files (
			hbl$options$HBASE_HOME,
			full.names=T,
			pattern=".*hbase.*\\.jar")
	
	# TODO: pig classpath, too?
	
	hconf <- Sys.getenv("HADOOP_CONF")
	
	if (hconf == "")
		hconf <- paste(hbl$options$HADOOP_HOME,"conf",sep="/")
	
	hbaseConf <- paste(hbl$options$HBASE_HOME,"conf",sep="/")
	

    hbl$classpath <- c(cp1,cp2,cp3,core,hb_core, hconf,hbaseConf)
	
	
	
	.jinit(classpath = hbl$classpath )	

	hbl$conf <- new(J("org.apache.hadoop.conf.Configuration"))
	hbl$conf <- J("org.apache.hadoop.hbase.HBaseConfiguration")$create(hbl$conf)
	
	# chd3u3 or later requred
	hbl$conf$setBoolean(hbl$consts$HBASE_CONNECTION_PER_CONFIG,F)
	
	hbl$queryClient <- new(J("com.inadco.hbl.client.HblQueryClient"),hbl$conf)
	
	hbl <<- hbl
	
}

hbl.prepare <- function (x, ...) UseMethod("prepare")
hbl.setParameter <- function (x, ...) UseMethod("setParameter")
hbl.execute <- function (x, ...) UseMethod ("execute")


# prepared query class 
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


prepare.hblquery <- function (q, value) {
	
	qstr <- as.character(value)
	q$q$prepare(qstr)
	
	#debug
	cat ("query prepared:", qstr)
	
	q
} 

setParameter.hblquery <- function (q, paramIndex, value ) {
	# rely on rJava conversions at this point
	q$q$setHblParameter(paramIndex,value);
	q
}

execute.hblquery <- function (q ) {
	rs <- q$q$execute() 
	r <- data.frame()
	aliases <- rs$aliases
	
	if (! rs$hasNext() ) {
		#todo: is there a more efficient way of doing this?
		for (alias in aliases) r[[alias]] <- character(0)
		return(r)
	}
	
	while ( rs$hasNext() ) {
		row <- rs$current()
		datarow <- list()
		datarow[[aliases]] <- row$getObject(aliases)
		rbind(r,datarow)
	}
	r
}


hbl.init()
