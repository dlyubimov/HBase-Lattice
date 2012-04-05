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
							"group by dim1")));
	
	timerange <- strptime(c("2011/9/1", "2011/11/1"),
			"%Y/%m/%d")
	
	q$setParameter(0,timerange[1])
	q$setParameter(1,timerange[2])
	
	system.time(dframe <- q$execute()) 
	
	dframe
	
	
}



# test admin functions 
test2 <- function () { 

	# deploy/update HBL cube model from file
	hblAdmin <- hbl.HblAdmin$new(model.file.name="~/projects/github/hbase-lattice/sample/src/main/resources/example1.yaml")
	hblAdmin$deployCube()
	
	# drop existing cube 
	hblAdmin <- hbl.HblAdmin$new(cube.name="Example1")
	hblAdmin$dropCube()
	
}
