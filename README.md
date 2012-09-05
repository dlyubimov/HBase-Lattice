What it is 
=======
HBase-Lattice is an attempt to provide HBase-based BI "OLAP-ish" solution 
with primary goals of real time SLAs for queries, low latency of facts becoming 
available for query by means of parallelizable MapReduce incremental compiler, 
and emphasis on Time Series data.

Like OLAP, it has concepts of facts, measures, dimensions and dimension hierarchies. 
Data query is supported by means any of:

 1. declarative query api;
 2. simple select-like query language;
 3. HblInputFormat for distributed locality-sensitive bulk cube queries /exports; 
 4. R package to load summaries as R data frames for further processing.



Documentation 
============= 

Check out the docs folder. 

Build
=====

Current build sets dependencies on CDH3u3 stuff. (now HBase dependency is set on 0.92.1). Hadoop
and CDH3 dependencies are inherited transitively thru "ecoadapters" project.

It also depends on another our project, ecoadapters (same repo).

If you want to pull prebuilt artifacts in your maven project, you can use 
https://github.com/dlyubimov/dlyubimov-maven-repo/raw/master/releases for the maven repo url to pull from.
(there's also a snapshot repo but i don't rebuild snapshots regularly enough so probably 
local snapshot build will be a better choice for anyone who wants to try out the HEAD of trunk.)

all released builds there are tagged in the git (see tags).

The R package builds when -DR option is specified to maven. 
However, there are additional requirements for building R artifact 
(namely, R and dependent packages installed locally). R module releases 
are also available as prebuilt from the maven repository mentioned above.
They have "rpkg" maven classifier.

Contributors
============ 

Please check out [Features sought after](https://github.com/dlyubimov/HBase-Lattice/wiki/Features-sought-after) wiki page.


LICENSE
========

Apache 2.0
