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
	
	if ( nchar(hbl$options$HADOOP_HOME) ==0 )
		stop ("HADOOP_HOME not set");
	
	if ( nchar(hbl$options$HBL_HOME) == 0 ) 
		stop("HBL_HOME not set");
	
	
}
