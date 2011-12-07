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

Build
=====

Current build sets dependencies on CDH3u0 stuff. 

It also depends on another our project, ecoadapters (same repo).

If you want to pull prebuilt artifacts, you can use 
https://github.com/dlyubimov/dlyubimov-maven-repo/raw/master/releases for repo url to pull from.

all released builds there are tagged in the git (see tags).

Contributors
============ 

Please check out [Features sought after](https://github.com/dlyubimov/HBase-Lattice/wiki/Features-sought-after) wiki page.


LICENSE
========

Apache 2.0
