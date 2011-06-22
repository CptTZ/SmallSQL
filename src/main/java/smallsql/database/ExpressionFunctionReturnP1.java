/* =============================================================
 * SmallSQL : a free Java DBMS library for the Java(tm) platform
 * =============================================================
 *
 * (C) Copyright 2004-2006, by Volker Berlin.
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
 * ExpressionFunctionReturnP1.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 23.01.2005
 */
package smallsql.database;

/**
 * Super Class for functions which extends the metadata from the first parameter.
 * @author Volker Berlin
 */
abstract class ExpressionFunctionReturnP1 extends ExpressionFunction {

	
    boolean isNull() throws Exception{
        return param1.isNull();
    }
	

    Object getObject() throws Exception{
		if(isNull()) return null;
        int dataType = getDataType();
        switch(dataType){
	        case SQLTokenizer.BIT:
	        case SQLTokenizer.BOOLEAN:
	                return getBoolean() ? Boolean.TRUE : Boolean.FALSE;
	        case SQLTokenizer.BINARY:
	        case SQLTokenizer.VARBINARY:
	                return getBytes();
	        case SQLTokenizer.TINYINT:
	        case SQLTokenizer.SMALLINT:
	        case SQLTokenizer.INT:
	                return new Integer( getInt() );
	        case SQLTokenizer.BIGINT:
	                return new Long( getLong() );
	        case SQLTokenizer.REAL:
	                return new Float( getFloat() );
	        case SQLTokenizer.FLOAT:
	        case SQLTokenizer.DOUBLE:
	                return new Double( getDouble() );
	        case SQLTokenizer.MONEY:
	        case SQLTokenizer.SMALLMONEY:
	                return Money.createFromUnscaledValue( getMoney() );
	        case SQLTokenizer.NUMERIC:
	        case SQLTokenizer.DECIMAL:
	                return getNumeric();
	        case SQLTokenizer.CHAR:
	        case SQLTokenizer.NCHAR:
	        case SQLTokenizer.VARCHAR:
	        case SQLTokenizer.NVARCHAR:
	        case SQLTokenizer.LONGNVARCHAR:
	        case SQLTokenizer.LONGVARCHAR:
	        		return getString();
	        case SQLTokenizer.LONGVARBINARY:
	                return getBytes();
			case SQLTokenizer.DATE:
			case SQLTokenizer.TIME:
			case SQLTokenizer.TIMESTAMP:
			case SQLTokenizer.SMALLDATETIME:
				return new DateTime( getLong(), dataType );
	        case SQLTokenizer.UNIQUEIDENTIFIER:
	                return getBytes();
	        default: throw createUnspportedDataType(param1.getDataType());
	    }
    }
	

	int getDataType() {
		return param1.getDataType();
	}

	
	int getPrecision() {
		return param1.getPrecision();
	}

	
	final int getScale(){
		return param1.getScale();
	}
}
