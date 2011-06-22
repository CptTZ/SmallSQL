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
 * ExpressionFunctionReturnP1Number.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 03.10.2005
 */
package smallsql.database;

abstract class ExpressionFunctionReturnP1Number extends ExpressionFunctionReturnP1 {


    final boolean getBoolean() throws Exception{
        return getDouble() != 0;
    }
	

	final int getInt() throws Exception {
		return Utils.long2int(getLong());
	}
	

    final long getLong() throws Exception{
        return Utils.double2long(getDouble());
    }
	

	final float getFloat() throws Exception {
		return (float)getDouble();
	}
	

    MutableNumeric getNumeric() throws Exception{
		if(param1.isNull()) return null;
		switch(getDataType()){
			case SQLTokenizer.INT:
				return new MutableNumeric(getInt());
			case SQLTokenizer.BIGINT:
				return new MutableNumeric(getLong());
			case SQLTokenizer.MONEY:
				return new MutableNumeric(getMoney(), 4);
			case SQLTokenizer.DECIMAL:
				MutableNumeric num = param1.getNumeric();
				num.floor();
				return num;
			case SQLTokenizer.DOUBLE:
				return new MutableNumeric(getDouble());
			default:
				throw new Error();
		}
    }
	
	
    long getMoney() throws Exception{
        return Utils.doubleToMoney(getDouble());
    }
	
	
	String getString() throws Exception {
		if(isNull()) return null;
		return getObject().toString();
	}
	
	
	final int getDataType() {
		return ExpressionArithmetic.getBestNumberDataType(param1.getDataType());
	}

	


}
