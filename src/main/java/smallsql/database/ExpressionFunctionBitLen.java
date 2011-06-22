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
 * ExpressionFunctionBitLen.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 24.06.2006
 */
package smallsql.database;


/**
 * BIT_LEN(str): Length in bits of a string.
 * <p>
 * Reflects the space occupied by one char = 2 bytes.
 * 
 * @author Saverio Miroddi
 */
final class ExpressionFunctionBitLen extends ExpressionFunctionReturnInt {
	private static final int BYTES_PER_CHAR = 2;

	final int getFunction() {
		return SQLTokenizer.BITLEN;
	}


    boolean isNull() throws Exception {
        return param1.isNull();
    }


	final int getInt() throws Exception {
        if(isNull()) return 0;

        String str = param1.getString();
		
		return str.length() * BYTES_PER_CHAR * 8;
	}
}
