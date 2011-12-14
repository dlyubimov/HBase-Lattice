/*
 * 
 *  Copyright © 2010, 2011 Inadco, Inc. All rights reserved.
 *  
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *  
 *         http://www.apache.org/licenses/LICENSE-2.0
 *  
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *  
 *  
 */

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
    private IErrorReporter errorReporter = null;
	
	public void setHblParams ( Map<Integer,Object> params ) {
		hblParams = params; 
	}
	public void setQueryVisitor ( QueryVisitor qVisitor ) { 
		this.qVisitor = qVisitor;
	}
	protected void mismatch(IntStream input, int ttype, BitSet follow)
    throws RecognitionException {
        throw new MismatchedTokenException(ttype, input);
    }
    public Object recoverFromMismatchedSet(IntStream input,
                                           RecognitionException e,
                                           BitSet follow)
                                               throws RecognitionException    {
        throw e;
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

// Alter code generation so catch-clauses get replaced with
// this action.
//@rulecatch {
//catch (RecognitionException e) {
//throw e;
//}
// }

select
scope Visitor; 
@init {
	qVisitor.reset(); 
    hblParamCnt =0;
}
	: 	^( SELECT exprList fromClause=. whereClause? groupClause? ) 
	{ qVisitor.visitSelect ( $exprList.start, $fromClause, $whereClause.start, $groupClause.start ); }
	; 


exprList
scope Visitor;
@init {
	$Visitor::selectionExpr = true; 
}
    :   ^(SELECTION_LIST selectExpr+)      
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
    
aggrFunc returns [String measure, String funcName]
scope Visitor;
@init {
	if ( $Visitor[-1]::selectionExpr ) {  
	   $Visitor::selectionExpr=true; 
	}
}
    :   ^( FUNC func=id fparam=id ) { 
    	$measure=$fparam.nameVal;
    	$funcName=$func.nameVal; 
    	}
    ;   
        
selectExpr 
    :   ^( SEL_EXPR dim=id alias=id? ) { 
    	String dimAlias = $alias.nameVal;
    	if ( dimAlias == null ) dimAlias = $dim.nameVal;
    	qVisitor.visitSelectExpressionAsID($dim.nameVal, dimAlias); }
    	
    |   ^( SEL_EXPR aggrFunc alias=id? ) { 
    	String aggrAlias = $alias.nameVal;
    	if ( aggrAlias == null ) 
    	   aggrAlias = $aggrFunc.funcName + "_" + $aggrFunc.measure;
    	qVisitor.visitSelectExpressionAsAggrFunc($aggrFunc.funcName, $aggrFunc.measure, aggrAlias); }
    ;   

id  returns [ String nameVal ]
@after {
	if ( $Visitor::groupBy ) 
	   qVisitor.visitGroupDimension($nameVal); 
}
    :   ID { 
    	   $nameVal = $ID.text;
    	}
    |   param { $param.val instanceof String}? {
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

     
		
	 
