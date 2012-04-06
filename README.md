What it is 
=======
HBase-Lattice is an attempt to provide HBase-based BI "OLAP-ish" solution 
with primary goals of real time SLAs for queries, low latency of facts becoming 
available for query by means of parallelizable MapReduce incremental compiler, 
and emphasis on Time Series data.

Like OLAP, it has concepts of facts, measures, dimensions and dimension hierarchies. 
Data query is supported by means of 1) declarative query api or 2) simple 
select-like query language. 



Documentation 
============= 

Check out the docs folder. 

NEWS
----

Now has R package, "hblr", to run hbl queries form inside R directly into HBL . 
R setup package is fairly simple (see ecor/install.sh to compile and install from sources). 

This depends on two packages: 1) rJava (standard R-java integration package) 
and 2) ecor (from ecoadapters project, tested with ecor-0.4.0-snapshot at this point of time) 

Compiled R packages are also available as maven artifact from the maven repository configured 
in pom's. See documentation and test.R for example of use.


Build
=====

Current build sets dependencies on CDH3u3 stuff. 

It also depends on another our project, ecoadapters (same repo).

If you want to pull prebuilt artifacts in your maven project, you can use 
https://github.com/dlyubimov/dlyubimov-maven-repo/raw/master/releases for tge maven repo url to pull from.
(there's also a snapshot repo but i don't rebuild snapshots regularly enough so probably 
local snapshot build will be a better choice for anyone who wants to try out the HEAD of trunk.)

all released builds there are tagged in the git (see tags).

Contributors
============ 

Please check out [Features sought after](https://github.com/dlyubimov/HBase-Lattice/wiki/Features-sought-after) wiki page.


LICENSE
========

Apache 2.0
