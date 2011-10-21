grammar HBLQueryAST;

options { 
  language = Java;
  output=AST;
}


tokens {
  SELECT = 'select';
  FROM = 'from';
  GROUP = 'group';
  BY = 'by';
  WHERE = 'where';
  SLICE = 'slice';
  IN = 'in';
  INF = 'inf';
  FUNC;
  SELECTION_LIST;
}

@header { 
  package com.inadco.hbl.hblquery;
  import java.util.Map;
  import java.util.HashMap;
}
@lexer::header { 
  package com.inadco.hbl.hblquery;
}

@members {
	Map<Integer,String> params = new HashMap<Integer,String>();
	
	public Map<Integer,String> getHblQueryParams() { 
		return params; 
	}
}

select 
	: 	SELECT^ selectExpr fromClause whereClause? groupClause?  
	;
	
selectExpr 
	: 	expr (',' expr )* -> ^(SELECTION_LIST expr+)	
	;
	
fromClause
	: 	FROM^ id
	; 
	
groupClause 	     
	:	GROUP^ BY! id (','! id)* 
	;
		
	
unaryFunc 
	:	ID '(' id ')' -> ^( FUNC id ) 
	;	
		
expr 	
	:	ID
	| 	unaryFunc
	| 	param
	;		
	
param 
	: 	'?'
	;	

whereClause
	: 	WHERE^ sliceSpec (','! sliceSpec*)
	;
	

/**
  this rule is to specify dimension slices. 
  the syntax is like "WHERE dim1=[0,2)"
  or, for exact hyperplane cut, something like "WHERE dim1=['123AEBCD'].
   
*/	
sliceSpec
	:	sliceId=id IN leftClosed=('[' | '(') left=value (',' right=value)? rightClosed=(']' | ')') -> 
		^( SLICE $sliceId $leftClosed $left $rightClosed $right )
	; 
	
value 
	: 	param | INT | FLOAT | STRING 
	| 	'-' INT
	|	'-' FLOAT
	;
	
id	
	: 	ID
	|	param
	;		  		

ID  :	('a'..'z'|'A'..'Z'|'_') ('a'..'z'|'A'..'Z'|'0'..'9'|'_')*
    ;

INT returns [Integer val] 
	:	'0'..'9'+ { $val= Integer.parseInt($text); }
    ;

FLOAT returns [Double val]
@after{
	if ( $val == null ) 
		$val=Double.parseDouble($text);
}
    :   ('0'..'9')+ '.' ('0'..'9')* EXPONENT? 
    |   '.' ('0'..'9')+ EXPONENT? 
    |   ('0'..'9')+ EXPONENT 
    |	INF { $val=Double.POSITIVE_INFINITY; } 
    ;

COMMENT
    :   '//' ~('\n'|'\r')* '\r'? '\n' {$channel=HIDDEN;}
    |   '/*' ( options {greedy=false;} : . )* '*/' {$channel=HIDDEN;}
    ;

WS  :   ( ' '
        | '\t'
        | '\r'
        | '\n'
        ) {$channel=HIDDEN;}
    ;

STRING returns [String val]
    :  '\'' ESCAPED_STR '\''
    	{ $val= $ESCAPED_STR.text; }
    ;
    
ESCAPED_STR 
	:    ( ESC_SEQ | ~('\\'|'\'') )*
	;

fragment
EXPONENT : ('e'|'E') ('+'|'-')? ('0'..'9')+ ;

fragment
HEX_DIGIT : ('0'..'9'|'a'..'f'|'A'..'F') ;

fragment
ESC_SEQ
    :   '\\' ('b'|'t'|'n'|'f'|'r'|'\"'|'\''|'\\')
    |   UNICODE_ESC
    |   OCTAL_ESC
    ;

fragment
OCTAL_ESC
    :   '\\' ('0'..'3') ('0'..'7') ('0'..'7')
    |   '\\' ('0'..'7') ('0'..'7')
    |   '\\' ('0'..'7')
    ;

fragment
UNICODE_ESC
    :   '\\' 'u' HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT
    ;
