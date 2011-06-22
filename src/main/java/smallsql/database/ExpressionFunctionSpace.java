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
 * ExpressionFunctionSpace.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 23.06.2006
 */
package smallsql.database;


/**
 * @author Volker Berlin
 */
public class ExpressionFunctionSpace extends ExpressionFunctionReturnString {

	final int getFunction() {
		return SQLTokenizer.SPACE;
	}


    boolean isNull() throws Exception {
        return param1.isNull() || param1.getInt()<0;
    }
    
    
    final String getString() throws Exception {
		if(isNull()) return null;
        int size = param1.getInt();
        if(size < 0){
            return null;
        }
		char[] buffer = new char[size];
        for(int i=0; i<size; i++){
            buffer[i] = ' ';
        }
		return new String(buffer);
	}


	final int getDataType() {
		return SQLTokenizer.VARCHAR;
	}
}
