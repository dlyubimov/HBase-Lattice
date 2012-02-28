# TODO: Add comment
# 
# Author: dmitriy
###############################################################################


test.my <- function() { }


setwd( getSrcDirectory(test.my))
setwd("..")
setwd("R")
source("hblr.R")






test1 <- function () {
	
	q <- hbl.hblquery("select dim1, COUNT(impCnt) as impCnt from Example1 group by dim1")
	df <- hbl.execute(q); df
	
}



test1()
