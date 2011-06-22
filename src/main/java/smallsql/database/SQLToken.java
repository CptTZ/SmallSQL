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
 * SQLToken.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.database;

/**
 * @author Volker Berlin
 *
 */
class SQLToken{
	int value;
	int offset;  // the start offset the token in the SQL string
	int length;  // the length of the token
	String name;

	SQLToken (int value, int tokenStart, int tokenEnd){
		this.value  = value;
		this.offset = tokenStart;
		this.length = tokenEnd-tokenStart;
	}

	/**
	 * Constructor used for quoted strings 
	 */
	SQLToken (String name, int value, int tokenStart, int tokenEnd){
		this.value  = value;
		this.offset = tokenStart;
		this.length = tokenEnd-tokenStart;
		this.name   = name;
	}

	String getName(char[] sql){
		if(name != null) return name;
		return new String( sql, offset, length );
	}
}
