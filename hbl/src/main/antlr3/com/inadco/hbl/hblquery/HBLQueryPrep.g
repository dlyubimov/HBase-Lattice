tree grammar HBLQueryPrep; 

options {
	tokenVocab=HBLQueryAST;
	ASTLabelType=CommonTree; 
}

@header { 
  package com.inadco.hbl.hblquery;
  import java.util.Map;
  import java.util.HashMap;
  import com.inadco.hbl.client.AggregateQuery;
}

@members {
	
	private Map<Integer,String> hblParams;
	private AggregateQuery aggregateQuery;
	private int hblParamCnt;
	
	public void setHblParams ( Map<Integer,String> params ) {
		hblParams = params; 
		hblParamCnt =0;
	}
	public void setAggregateQuery ( AggregateQuery query ) { 
		aggregateQuery = query;
	}
}

select
	: 	^( SELECT exprList fromClause whereClause? groupClause? )
	; 

exprList 
	: ^( SELECTION_LIST expr+ )	
	;
	
fromClause
	: 	^( FROM id )
	; 
	
groupClause 	     
	:	^( GROUP  id+ ) 
	;
		
	
unaryFunc 
	:	^( FUNC id ) 
	;	
		
expr 	
	:	ID
	| 	unaryFunc
	| 	param
	;		
	
param returns [String val]
	: 	'?' { val = hblParams.get(hblParamCnt++); }
	;	

whereClause
	: 	^( WHERE sliceSpec+ )
	;
	

/**
  this rule is to specify dimension slices. 
  the syntax is like "WHERE dim1=[0,2)"
  or, for exact hyperplane cut, something like "WHERE dim1=['123AEBCD'].
   
*/	
sliceSpec
	:	^( SLICE sliceId=id leftClosed=('[' | '(') left=value rightClosed=(']' | ')') right=value? )
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
	
	 
