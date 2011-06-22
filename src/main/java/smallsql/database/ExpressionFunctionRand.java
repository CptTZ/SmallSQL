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
 * ExpressionFunctionRand.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.database;

import java.util.Random;


final class ExpressionFunctionRand extends ExpressionFunctionReturnFloat {

	final static private Random random = new Random(); 
	
	
    final int getFunction(){ return SQLTokenizer.RAND; }
	

    boolean isNull() throws Exception{
        return getParams().length == 1 && param1.isNull();
    }
	

    final double getDouble() throws Exception{
		if(getParams().length == 0)
			return random.nextDouble();
		if(isNull()) return 0;
		return new Random(param1.getLong()).nextDouble(); 
    }
}