tree grammar HBLQueryPrep; 

options {
	tokenVocab=HBLQueryAST;
	ASTLabelType=CommonTree; 
}

tokens{
    DIM;
}

scope Visitor {
    boolean selectionExpr;
    boolean visitMeasures; 
    boolean visitDimensions;
    boolean groupBy;
    
}


@header { 
  package com.inadco.hbl.hblquery;
  import java.util.Map;
  import java.util.HashMap;
  import com.inadco.hbl.client.impl.QueryVisitor;
}

@members {
	
	private Map<Integer,Object> hblParams;
	private QueryVisitor qVisitor;
	private int hblParamCnt;
	
	public void setHblParams ( Map<Integer,Object> params ) {
		hblParams = params; 
		hblParamCnt =0;
	}
	public void setQueryVisitor ( QueryVisitor qVisitor ) { 
		this.qVisitor = qVisitor;
	}
}

select
scope Visitor; 
	: 	^( SELECT exprList fromClause=. whereClause? groupClause? ) 
	{ qVisitor.visitSelect ( $exprList.start, $fromClause, $whereClause.start, $groupClause.start ); }
	; 


exprList
scope Visitor;
@init {
	$Visitor::selectionExpr = true; 
	$Visitor::visitDimensions=true;
}
    :   ^(SELECTION_LIST expr+)      
    ;

whereClause
    :   ^( WHERE sliceSpec+ )
    ;

sliceSpec
    :  ^( SLICE id leftBoundType left=value rightBoundType right=value? ) 
    {
    	qVisitor.visitSlice ($id.nameVal, 
    	   $leftBoundType.open, 
    	   $left.val,
    	   $rightBoundType.open, 
    	   $right.val); 
    }
    ; 

leftBoundType returns [boolean open]
    :   LEFTCLOSED { open=false; }
    |   LEFTOPEN {open=true; }
    ;
    
rightBoundType returns [boolean open]
    :  RIGHTCLOSED {open=false; }
    |  RIGHTOPEN {open=true; }
    ;

groupClause 
scope Visitor;
@init {
	$Visitor::groupBy=true; 
}
    :   ^( GROUP id+ )
    ;    
    
aggrFunc 
scope Visitor;
@init {
	if ( $Visitor[-1]::selectionExpr ) {  
	   $Visitor::visitMeasures = true;
	   $Visitor::selectionExpr=true; 
	}
}
    :   ^( FUNC id ) 
    ;   
        
expr 
    :   id 
    |   aggrFunc 
    ;   

id  returns [String nameVal ]
@after {
	if ( $Visitor::visitDimensions ) 
	   qVisitor.visitDim($nameVal);
	if ( $Visitor::visitMeasures ) 
	   qVisitor.visitMeasure($nameVal);
	if ( $Visitor::groupBy ) 
	   qVisitor.visitGroupDimension($nameVal); 
}
    :   ID { 
    	   $nameVal = $ID.text;
    	}
    |   param {
    	   $nameVal = (String) $param.val;
        }
    ;               
        
    
param returns [Object val]
    :   '?'
        { $val=hblParams.get(hblParamCnt++); }
    ;   
    
value returns [Object val] 
    :   param { $val= $param.val; } 
    | INT { $val= Integer.parseInt($text); }
    | FLOAT { $val= Double.parseDouble($text); } 
    | STRING { $val= $STRING.text.substring(1,$STRING.text.length()-1); }
    |   '-' INT { $val= Integer.parseInt ($text); }
    |   '-' FLOAT { $val= Double.parseDouble($text); }
    ;

     
		
	 
