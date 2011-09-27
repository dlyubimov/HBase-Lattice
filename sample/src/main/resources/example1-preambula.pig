-- this is example preambula loading a set of dimension values and measure values.

INPUT = load '$input' using PigStorage() as (timeDim:long, idDim1:bytearray, idDim2:byteArray, measure1:double, measure2:double );

