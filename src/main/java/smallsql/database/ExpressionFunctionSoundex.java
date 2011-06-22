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
 * ExpressionFunctionSoundex.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 18.06.2006
 */
package smallsql.database;


/**
 * @author Volker Berlin
 */
public class ExpressionFunctionSoundex extends ExpressionFunctionReturnP1StringAndBinary {

	final int getFunction() {
		return SQLTokenizer.SOUNDEX;
	}


	final boolean isNull() throws Exception {
		return param1.isNull();
	}


	final byte[] getBytes() throws Exception{
        throw createUnspportedConversion(SQLTokenizer.BINARY);
	}
	
	
	final String getString() throws Exception {
		if(isNull()) return null;
        String input = param1.getString();
        return getString(input);
    }
     
    
    static String getString(String input){
        char[] output = new char[4];
        int idx = 0;
        input = input.toUpperCase();
        if(input.length()>0){
            output[idx++] = input.charAt(0);
        }
        char last = '0';
        for(int i=1; idx<4 && i<input.length(); i++){
            char c = input.charAt(i);
            switch(c){
            case 'B':
            case 'F':
            case 'P':
            case 'V':
                c = '1';
                break;
            case 'C':
            case 'G':
            case 'J':
            case 'K':
            case 'Q':
            case 'S':
            case 'X':
            case 'Z':
                c = '2';
                break;
            case 'D':
            case 'T':
                c = '3';
                break;
            case 'L':
                c = '4';
                break;
            case 'M':
            case 'N':
                c = '5';
                break;
            case 'R':
                c = '6';
                break;
            default:
                c = '0';
                break;
            }
            if(c > '0' && last != c){
                output[idx++] = c;
            }
            last = c;
        }
        for(; idx<4;){
            output[idx++] = '0';
            
        }
		return new String(output);
	}
    
    
    int getPrecision(){
        return 4;
    }
}
