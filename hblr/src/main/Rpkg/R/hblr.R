# TODO: R wrapper for running hbl queries etc. using rJava
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
	hbl$options$PIG_HOME <- Sys.getenv("PIG_HOME");
	
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
	
	pigcp <- if ( length(hbl$PIG_HOME)>0 ) 
		list.files(
				paste(hbl$PIG_HOME,"lib",sep="/"),
				full.names=T,
				pattern = "\\.jar$"
				)
	
	
	hb_core <- list.files (
			hbl$options$HBASE_HOME,
			full.names=T,
			pattern=".*hbase.*\\.jar")
	
	# TODO: pig classpath, too?
	
	hconf <- Sys.getenv("HADOOP_CONF")
	
	if (hconf == "")
		hconf <- paste(hbl$options$HADOOP_HOME,"conf",sep="/")
	
	hbaseConf <- paste(hbl$options$HBASE_HOME,"conf",sep="/")
	

  	hbl$classpath <- c(cp1,cp2,cp3,core,hb_core, hconf,hbaseConf, pigcp)
	
	.jinit(classpath = hbl$classpath )	

	hbl$conf <- new(J("org.apache.hadoop.conf.Configuration"))
	hbl$conf <- J("org.apache.hadoop.hbase.HBaseConfiguration")$create(hbl$conf)
	
	# chd3u3 or later requred
	hbl$conf$setBoolean(hbl$consts$HBASE_CONNECTION_PER_CONFIG,F)
	
	hbl$queryClient <- new(J("com.inadco.hbl.client.HblQueryClient"),hbl$conf)
	
	hbl <<- hbl
	
}


#############################################
# hbl query methods for R class "hblquery"  #
#############################################

#Use to re-prepare query obtained thru hbl.hblquery() 
hbl.prepare <- function (x, ...) UseMethod("prepare")
#set parameters (there's some todo here probably still)
hbl.setParameter <- function (x, ...) UseMethod("setParameter")
#execute query
hbl.execute <- function (x, ...) UseMethod ("execute")


# prepared query class constructor  
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
	datarow <- hbl._convertRS(aliases,rs$current())
	r <- data.frame(datarow, stringsAsFactors=F)

	while ( rs$hasNext() ) {
		rs$"next"()
		datarow <- hbl._convertRS(aliases,rs$current())
		r<- rbind(r,datarow)
	}
	
	
	r 
}

# convert a result set row to a list of values 
# denoted by 
hbl._convertRS <- function (aliases, hblrow ) {
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
hbl.dropCube <- function (x,...) UseMethod("dropCube")
hbl.deployCube <- function (x,...) UseMethod("deployCube")
hbl.saveModel <- function (x,...) UseMethod("saveModel")

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

##################################
# initialization                 #
##################################
hbl.init()
