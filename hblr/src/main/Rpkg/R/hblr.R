# TODO: Add comment
# 
# Author: dmitriy
###############################################################################

hbl = new.env();
hbl$options = list();

library("rJava");

hbl.init = function () {
	
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
			
	cp3 <- list.files (
			paste(hbl$options$HBL_HOME,"hbl/target/", sep="/"),
			full.names = T,
			pattern="\\.jar$"
			)
			
#	cp4 <- list.files( 
#			paste(hbl$options$HBASE_HOME,"/lib",sep=""),
#			full.names = T,
#			pattern="\\.jar$")
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

	hbl$conf <- new(J("org.apache.hadoop.conf.Configuration"));
	hbl$conf <- J("org.apache.hadoop.hbase.HBaseConfiguration")$create(hbl$conf);
	
}

