# TODO: Add comment
# 
# Author: dmitriy
###############################################################################

test1 <- function () {
	
	library(hblr)
	
	q <- hbl.HblQuery$new("select dim1, COUNT(impCnt) as impCnt from Example1 group by dim1")
	dframe <- q$execute() 
	dframe
  
    hblAdmin <- HblAdmin$new(model.file.name="~/projects/github/hbase-lattice/sample/src/main/resources/example1.yaml")
    hblAdmin <- HblAdmin$new(cube.name="Example1")
	
}




test1()
