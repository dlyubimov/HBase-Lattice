%declare cubeModel '$hbl:{cubeModel}'

set pig.noSplitCombination true; -- don't combine small files in one split 

DEFINE hbl_m2d com.inadco.hbl.piggybank.Measure2Double('$cubeModel');
DEFINE hbl_d2k com.inadco.hbl.piggybank.Dimensions2CuboidKey('$cubeModel');

$hbl:{cuboidStoreDefs}

$hbl:{preambula}

HBLEVAL = foreach $hbl:{inputRelation} generate *;

$hbl:{body}



