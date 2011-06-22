/* =============================================================
 * SmallSQL : a free Java DBMS library for the Java(tm) platform
 * =============================================================
 *
 * (C) Copyright 2004-2007, by Volker Berlin.
 *
 * Project Info:  http://www.smallsql.de/
 *
 * This library is free software; you can redistribute it and/or modify it 
 * under the terms of the GNU Lesser General Public License as published by 
 * the Free Software Foundation; either version 2.1 of the License, or 
 * (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful, but 
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY 
 * or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public 
 * License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, 
 * USA.  
 *
 * [Java is a trademark or registered trademark of Sun Microsystems, Inc. 
 * in the United States and other countries.]
 *
 * ---------------
 * ExpressionFunction.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.database;

import java.sql.*;
import smallsql.database.language.Language;

/**
 * This is the base class for all functions. To add a new fuction you need<p>
 * 1.) Add a new constant to the class SQLTokenizer.<p>
 * 2.) Add a mapping of the function name keyword to the function constant in the class SQLTokenizer.<p>
 * 3.) Extends a class from ExpressionFunction and implemets the function logic.<p>
 * 4.) Add a case to the switch in SQLParser.function().<p>
 */

abstract class ExpressionFunction extends Expression {

    Expression param1;
    Expression param2;
    Expression param3;
    Expression param4;

	ExpressionFunction(){
		super(FUNCTION);
	}

    // setzt die Funktionsnummer z.B. bei abs(5) --> SQLTokenizer.ABS
    abstract int getFunction();

    byte[] getBytes() throws Exception{
        return ExpressionValue.getBytes(getObject(), getDataType());
    }

    void setParams( Expression[] params ){
        super.setParams( params );
        if(params.length >0) param1 = params[0] ;
        if(params.length >1) param2 = params[1] ;
        if(params.length >2) param3 = params[2] ;
        if(params.length >3) param4 = params[3] ;
    }

	final void setParamAt( Expression param, int idx){
		switch(idx){
			case 0:
				param1 = param;
				break;
			case 1:
				param2 = param;
				break;
			case 2:
				param3 = param;
				break;
			case 3:
				param4 = param;
				break;
		}
		super.setParamAt( param, idx );
	}
	

	/**
	 * Is used in GroupResult.
	 */
	public boolean equals(Object expr){
		if(!super.equals(expr)) return false;
		if(!(expr instanceof ExpressionFunction)) return false;
		return ((ExpressionFunction)expr).getFunction() == getFunction();
	}

	
    /**
     * Create a SQLException that the current function does not support the specific data type.
     * @param dataType A data type const from SQLTokenizer.
     */
	SQLException createUnspportedDataType( int dataType ){
		Object[] params = {
				SQLTokenizer.getKeyWord(dataType),
				SQLTokenizer.getKeyWord(getFunction())
		};
        return SmallSQLException.create(Language.UNSUPPORTED_DATATYPE_FUNC, params);
    }

    /**
     * Create a SQLException that the current function can not convert the specific data type.
     * @param dataType A data type const from SQLTokenizer.
     */
    SQLException createUnspportedConversion( int dataType ){
    	Object[] params = {
    			SQLTokenizer.getKeyWord(dataType),
    			SQLTokenizer.getKeyWord(getFunction())
    	};
        return SmallSQLException.create(Language.UNSUPPORTED_CONVERSION_FUNC, params);
    }
}