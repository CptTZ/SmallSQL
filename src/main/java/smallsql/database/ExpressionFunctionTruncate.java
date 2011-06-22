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
 * ExpressionFunctiontruncate.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.database;


final class ExpressionFunctionTruncate extends ExpressionFunctionReturnP1Number {

    final int getFunction(){ return SQLTokenizer.TRUNCATE; }

    boolean isNull() throws Exception{
        return param1.isNull() || param2.isNull();
    }
	

    final double getDouble() throws Exception{
		if(isNull()) return 0;
		final int places = param2.getInt();
		double value = param1.getDouble();
		long factor = 1;
		if(places > 0){
			for(int i=0; i<places; i++){
				factor *= 10;
			}
			value *= factor;
		}else{
			for(int i=0; i>places; i--){
				factor *= 10;
			}
			value /= factor;
		}
        value -= value % 1; //modulo also work with values larger long
		if(places > 0){
			value /= factor;
		}else{
			value *= factor;
		}
		return value;
    }
	

}