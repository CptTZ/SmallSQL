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
 * ExpressionFunctionReplace.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 23.06.2006
 */
package smallsql.database;

import java.io.ByteArrayOutputStream;


/**
 * @author Volker Berlin
 */
public class ExpressionFunctionReplace extends ExpressionFunctionReturnP1StringAndBinary {

	final int getFunction() {
		return SQLTokenizer.REPLACE;
	}


	final boolean isNull() throws Exception {
		return param1.isNull() || param2.isNull() || param3.isNull();
	}


	final byte[] getBytes() throws Exception{
		if(isNull()) return null;
        byte[] str1 = param1.getBytes();
        byte[] str2  = param2.getBytes();
        int length = str2.length;
        if(length == 0){
            return str1;
        }
        byte[] str3  = param3.getBytes();
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int idx1 = 0;
        int idx2 = Utils.indexOf(str2,str1,idx1);
        while(idx2 > 0){
            buffer.write(str1,idx1,idx2-idx1);
            buffer.write(str3);
            idx1 = idx2 + length;
            idx2 = Utils.indexOf(str2,str1,idx1);
        }
        if(idx1 > 0){
            buffer.write(str1,idx1,str1.length-idx1);
            return buffer.toByteArray();
        }
        return str1;
	}
	
	
	final String getString() throws Exception {
		if(isNull()) return null;
		String str1 = param1.getString();
		String str2  = param2.getString();
        int length = str2.length();
        if(length == 0){
            return str1;
        }
        String str3  = param3.getString();
        StringBuffer buffer = new StringBuffer();
        int idx1 = 0;
        int idx2 = str1.indexOf(str2,idx1);
        while(idx2 >= 0){
            buffer.append(str1.substring(idx1,idx2));
            buffer.append(str3);
            idx1 = idx2 + length;
            idx2 = str1.indexOf(str2,idx1);
        }
        if(idx1 > 0){
            buffer.append(str1.substring(idx1));
            return buffer.toString();
        }
		return str1;
	}


    int getPrecision() {
        return SSResultSetMetaData.getDataTypePrecision( getDataType(), -1 );
    }

}
