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


LICENSE
========

Apache 2.0
