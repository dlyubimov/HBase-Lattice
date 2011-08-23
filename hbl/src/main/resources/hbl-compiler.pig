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

%declare cubeModel '$hbl:{cubeModel}'

set pig.noSplitCombination true; -- don't combine small files in one split 

DEFINE hbl_m2d com.inadco.hbl.piggybank.Measure2Double('$cubeModel');
DEFINE hbl_d2k com.inadco.hbl.piggybank.Dimensions2CuboidKey('$cubeModel');

$hbl:{cuboidStoreDefs}

$hbl:{preambula}

HBLEVAL = foreach $hbl:{inputRelation} generate *;

$hbl:{body}



