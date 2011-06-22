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
 * ExpressionFunctionSubstring.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 23.06.2004
 */
package smallsql.database;

import smallsql.database.language.Language;

/**
 * @author Volker Berlin
 */
final class ExpressionFunctionSubstring extends ExpressionFunctionReturnP1StringAndBinary {


	final int getFunction() {
		return SQLTokenizer.SUBSTRING;
	}


	final boolean isNull() throws Exception {
		return param1.isNull() || param2.isNull() || param3.isNull();
	}


	final byte[] getBytes() throws Exception{
		if(isNull()) return null;
		byte[] bytes = param1.getBytes();
		int byteLen = bytes.length;
		int start  = Math.min( Math.max( 0, param2.getInt() - 1), byteLen);
		int length = param3.getInt();
		if(length < 0) 
			throw SmallSQLException.create(Language.SUBSTR_INVALID_LEN, new Integer(length));
		if(start == 0 && byteLen == length) return bytes;
		if(byteLen > length + start){
			byte[] b = new byte[length];
			System.arraycopy(bytes, start, b, 0, length);
			return b;		
		}else{
			byte[] b = new byte[byteLen - start];
			System.arraycopy(bytes, start, b, 0, b.length);
			return b;		
		}
	}
	
	
	final String getString() throws Exception {
		if(isNull()) return null;
		String str = param1.getString();
		int strLen = str.length();
		int start  = Math.min( Math.max( 0, param2.getInt() - 1), strLen);
		int length = param3.getInt();
		if(length < 0) 
			throw SmallSQLException.create(Language.SUBSTR_INVALID_LEN, new Integer(length));
		length = Math.min( length, strLen-start );
		return str.substring(start, start+length);
	}


}
