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
 * ExpressionFunctionSign.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 21.06.2004
 */
package smallsql.database;


/**
 * @author Volker Berlin
 */
final class ExpressionFunctionSign extends ExpressionFunctionReturnInt {


	final int getFunction() {
		return SQLTokenizer.SIGN;
	}


	final int getInt() throws Exception {
		if(param1.isNull()) return 0;
		switch(ExpressionArithmetic.getBestNumberDataType(param1.getDataType())){
			case SQLTokenizer.INT:
				int intValue = param1.getInt();
				if(intValue < 0)
					return -1;
				if(intValue > 0)
					return 1;
				return 0;
			case SQLTokenizer.BIGINT:
				long longValue = param1.getLong();
				if(longValue < 0)
					return -1;
				if(longValue > 0)
					return 1;
				return 0;
			case SQLTokenizer.MONEY:
				longValue = param1.getMoney();
				if(longValue < 0)
					return -1;
				if(longValue > 0)
					return 1;
				return 0;
			case SQLTokenizer.DECIMAL:
				return param1.getNumeric().getSignum();
			case SQLTokenizer.DOUBLE:
				double doubleValue = param1.getDouble();
				if(doubleValue < 0)
					return -1;
				if(doubleValue > 0)
					return 1;
				return 0;
			default:
				throw new Error();
		}
	}
}
