-- this is example preambula loading a set of dimension values and measure values.
-- the compiler recognizes the schema of a predefined incremental input 
-- where relation name of the input is set on the compiler bean, and by default 
-- is HBL_INPUT. This relation must contain attributes with pig schema names 
-- same as correspondent dimension and measure names in the cube model (defined in yaml file).

-- need protobuf-2.3.0.jar. Assuming default maven location...

INP = load '$input' using 
  com.inadco.ecoadapters.pig.SequenceFileProtobufLoader(
  'com.inadco.hb.example1.codegen.Example1\$CompilerInput');

-- form some irregular samples to populate irregular sampling aggregates

HBL_INPUT = foreach INP generate *, 
TOTUPLE(impCnt, impressionTime) as impTimeSeries, -- time series measure for impression events
TOTUPLE(click, impressionTime ) as clickTimeSeries; -- time series measure for click events