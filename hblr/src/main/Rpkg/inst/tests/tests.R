# TODO: Add comment
# 
# Author: dmitriy
###############################################################################

test1 <- function () {
	
	library(hblr)
	
	system.time(
			q <- hbl.HblQuery$new(c("select dim1, COUNT(impCnt) ",
							"as impCnt from Example1 ",
							"where impressionTime in [?,?) ",
							',dim1 in [?] ',
							"group by dim1")));
	
	timerange <- strptime(c("2011/9/1 PST", "2011/11/1 PST"),
			"%Y/%m/%d %z")
	
	q$setParameter(0,timerange[1])
	q$setParameter(1,timerange[2])
	q$setParameter(2,"1")
	
	system.time(dframe <- q$execute()) 
	
	dframe
	
	
}
test2 <- function() { 
	library(hblr)
	library(ggplot2)
	
	q <- hbl.HblQuery$new(c("select dim1, COUNT(impCnt) ",
					"as impCnt from Example1 ",
					"where impressionTime in [?,?) ",
					"group by dim1"))
	
	
	timerange <- as.numeric(strptime(c("2011-9-1", "2012-7-1"),
					"%Y-%m-%d", tz="PST"))
	# make timeseries grid with 1 day intervals and convert to numeric
	times <- as.POSIXct(seq(timerange[1],timerange[2],by=3600*24),
			origin="1970-01-01", tz="PST")

	dframe <- data.frame()
	
	# request all timeseries data and build into data frame
	# at 1 day grid 
	for ( i in 1:length(times) ) {
		t <- times[i]
		q$setParameter(0,t)
		q$setParameter(1,t+3600*24)
		r <- q$execute()
		r$time <- rep(t,nrow(r))
		dframe <- rbind(dframe,r)
	}
	
	#plot timeseries graph over that period
	try(dev.off())
	ggplot( data=dframe)+
			facet_wrap(~dim1,ncol=1)+
			geom_histogram(aes(x=time, weight=impCnt),binwidth=24*3600)
	
	
	
}



# test admin functions 
test3 <- function () { 

	# deploy/update HBL cube model from file
	hblAdmin <- hbl.HblAdmin$new(model.file.name="~/projects/github/hbase-lattice/sample/src/main/resources/example1.yaml")
	hblAdmin$deployCube()
	
	# drop existing cube 
	hblAdmin <- hbl.HblAdmin$new(cube.name="Example1")
	hblAdmin$dropCube()
	
}
