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
 * ExpressionFunctionInsert.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 17.06.2006
 */
package smallsql.database;

import java.io.ByteArrayOutputStream;
import smallsql.database.language.Language;

/**
 * @author Volker Berlin
 */
public class ExpressionFunctionInsert extends ExpressionFunctionReturnP1StringAndBinary {

	final int getFunction() {
		return SQLTokenizer.INSERT;
	}


	final boolean isNull() throws Exception {
		return param1.isNull() || param2.isNull() || param3.isNull() || param4.isNull();
	}


	final byte[] getBytes() throws Exception{
        if(isNull()) return null;
        byte[] bytes = param1.getBytes();
        int start  = Math.min(Math.max( 0, param2.getInt() - 1), bytes.length );
        int length = Math.min(param3.getInt(), bytes.length );
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        buffer.write(bytes,0,start);
        buffer.write(param4.getBytes());
        if(length < 0) 
            throw SmallSQLException.create(Language.INSERT_INVALID_LEN, new Integer(length));
        buffer.write(bytes, start+length, bytes.length-start-length);
        return buffer.toByteArray();
	}
	
	
	final String getString() throws Exception {
		if(isNull()) return null;
		String str = param1.getString();
        int start  = Math.min(Math.max( 0, param2.getInt() - 1), str.length() );
		int length = Math.min(param3.getInt(), str.length() );
        StringBuffer buffer = new StringBuffer();
        buffer.append(str.substring(0,start));
        buffer.append(param4.getString());
        if(length < 0) 
            throw SmallSQLException.create(Language.INSERT_INVALID_LEN, new Integer(length));
        buffer.append(str.substring(start+length));
		return buffer.toString();
	}


    int getPrecision() {
        return param1.getPrecision()+param2.getPrecision();
    }

    

}
