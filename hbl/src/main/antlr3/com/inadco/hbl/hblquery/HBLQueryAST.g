grammar HBLQueryAST;

options { 
  language = Java;
  output=AST;
}


tokens {
  AS = 'as';
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
  SEL_EXPR;
  LEFTOPEN;
  LEFTCLOSED;
  RIGHTOPEN;
  RIGHTCLOSED;
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
    private IErrorReporter errorReporter = null;
	
	public Map<Integer,String> getHblQueryParams() { 
		return params; 
	}
    public void setErrorReporter(IErrorReporter errorReporter) {
        this.errorReporter = errorReporter;
    }
    public IErrorReporter getErrorReporter() {
        return errorReporter; 
    }
    public void emitErrorMessage(String msg) {
        if ( errorReporter != null ) errorReporter.reportError(msg);
    }
    public void reset() { 
        if ( errorReporter != null ) errorReporter.reset();
        super.reset();
    }
}

select 
	: 	SELECT^ selectExprList fromClause whereClause? groupClause? EOF! 
	;
	
selectExprList 
	: 	selectExpr (',' selectExpr )* -> ^(SELECTION_LIST selectExpr+)	
	;
	
fromClause
	: 	FROM^ id
	; 
	
groupClause 	     
	:	GROUP^ BY! id (','! id)* 
	;
		
	
unaryFunc 
	:	func=id '(' fparam=id ')' -> ^( FUNC $func $fparam ) 
	;	
		
selectExpr 	
	:	measure=id ( AS alias=id )? -> ^( SEL_EXPR $measure $alias? )   
	| 	unaryFunc ( AS id)? -> ^( SEL_EXPR unaryFunc id? )
	;		
	
param 
	: 	'?'
	;	

whereClause
	: 	WHERE^ sliceSpec (','! sliceSpec)*
	;
	

/**
  this rule is to specify dimension slices. 
  the syntax is like "WHERE dim1=[0,2)"
  or, for exact hyperplane cut, something like "WHERE dim1=['123AEBCD'].
   
*/	
sliceSpec
	:	sliceId=id IN leftBoundType left=value (',' right=value)? rightBoundType -> 
		^( SLICE $sliceId leftBoundType $left rightBoundType $right? )
	; 
	
leftBoundType 
	:	'[' -> LEFTCLOSED 
	|	'(' -> LEFTOPEN
	;
	
rightBoundType 
	:	']' -> RIGHTCLOSED 
	|  ')' -> RIGHTOPEN
	;
	
value 
	: 	param | INT | FLOAT
	|	STRING 
	| 	'-' INT
	|	'-' FLOAT
	;
	
id	
	: 	ID
	|	param
	;		  		



ID :	('a'..'z'|'A'..'Z'|'_') ('a'..'z'|'A'..'Z'|'0'..'9'|'_')*
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


/* TODO: work on string definitions. 
   for some reason original string lexema 
   did not work. 
   */
STRING returns [ String s ]
	:	'\'' SC '\'' { $s= $SC.text; }
	;

fragment
SC 	:	( options { greedy=true;}: ~'\'' )*
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
