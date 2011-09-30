-- this is example preambula loading a set of dimension values and measure values.
-- the compiler recognizes the schema of a predefined incremental input 
-- where relation name of the input is set on the compiler bean, and by default 
-- is HBL_INPUT. This relation must contain attributes with pig schema names 
-- same as correspondent dimension and measure names in the cube model (defined in yaml file).

-- need protobuf-2.3.0.jar. Assuming default maven location...

HBL_INPUT = load '$input' using 
  com.inadco.ecoadapters.pig.SequenceFileProtobufLoader(
  'com.inadco.hb.example1.codegen.Example1\$CompilerInput');

