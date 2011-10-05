--                                                                                       
--  Copyright © 2010, 2011 Inadco, Inc. All rights reserved.                             
--                                                                                       
--     Licensed under the Apache License, Version 2.0 (the "License");                   
--     you may not use this file except in compliance with the License.                  
--     You may obtain a copy of the License at                                           
--                                                                                       
--         http://www.apache.org/licenses/LICENSE-2.0                                    
--                                                                                       
--     Unless required by applicable law or agreed to in writing, software               
--     distributed under the License is distributed on an "AS IS" BASIS,                 
--     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.          
--     See the License for the specific language governing permissions and               
--     limitations under the License.                                                    
--                                                                                       
                                                                                       

-- template for cuboid computation. 
-- there's a number of substitutes here, obviously.

-- generate hbl_m2d("measure1",measure1),..,hbl_d2k(dim1,dim2,dim3) as HKEY
-- cuboid key evaluation produces a bag of possible cuboid keys (all of the same length). 
-- multiple keys are produced if there are hierarchies, to produce [ALL] keys.

GROUP_$hbl:{cuboidTable} = FOREACH HBLEVAL generate $hbl:{measuresEval},  FLATTEN($hbl:{cuboidKeyEval}) as HKEY ;

GR_$hbl:{cuboidTable}_10 = GROUP GROUP_$hbl:{cuboidTable} by HKEY.dimkey parallel $hbl:{parallel};

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

 