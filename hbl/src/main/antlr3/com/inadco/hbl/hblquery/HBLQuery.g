grammar HBLQuery;

import HBLQueryLex;

options
{
    language=Java;
}

@header { 
package com.inadco.hbl.hblquery;
}

hblquery 
  : ID
  ;
  