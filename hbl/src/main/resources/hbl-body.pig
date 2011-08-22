-- template for cuboid computation. 
-- there's a number of substitutes here, obviously.

-- generate hbl_m2d("measure1",measure1),..,hbl_d2k(dim1,dim2,dim3) as HKEY
-- cuboid key evaluation produces a bag of possible cuboid keys (all of the same length). 
-- multiple keys are produced if there are hierarchies, to produce [ALL] keys.

GROUP_$hbl:{cuboidTable} = FOREACH HBLEVAL generate $hbl:{measuresEval},  FLATTEN($hbl:{cuboidKeyEval}) as HKEY ;

GR_$hbl:{cuboidTable}_10 = GROUP GROUP_$hbl:{cuboidTable} by HKEY parallel $hbl:{parallel};

GR_$hbl:{cuboidTable} = foreach GR_$hbl:{cuboidTable}_10 generate *;

-- now we form tuples with results of aggregate functions inside 
-- such as TO_TUPLE(SUM(GR_table.measure1),COUNT(GR_table.measure1)) as measure1:(sum:double,count:double)
-- also hbase-get parts
M_$hbl:{cuboidTable}_10 = foreach GR_$hbl:{cuboidTable} generate group as HKEY, $hbl:{measureMetricEvals}, 
  get_$hbl:{cuboidTable}() as hbl_old;

M_$hbl:{cuboidTable}_20 = foreach M_$hbl:{cuboidTable}_10 generate HKEY, $hbl:{measureMetricMerges};

M_$hbl:{cuboidTable} = foreach M_$hbl:{cuboidTable}_20 generate *;

-- debug
describe M_$hbl:{cuboidTable};

store M_$hbl:{cuboidTable} into '$hbl:{workDir}/$hbl:{cuboidTable}' using BinStorage();

OUT_$hbl:{cuboidTable} = load '$hbl:{workDir}/$hbl:{cuboidTable}' using BinStorage() AS(
HKEY:bytearray, $hbl:{measureMetricsSchema} );

store OUT_$hbl:{cuboidTable} into '$hbl:{cuboidTable}' using store_$hbl:{cuboidTable};



-- end of cuboid template.

 