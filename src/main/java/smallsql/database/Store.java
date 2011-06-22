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
 * Store.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.database;

import java.sql.*;
/**
 * @author Volker Berlin
 *
 */
abstract class Store {
	
	static final Store NULL = new StoreNull();
	static final Store NOROW= new StoreNoCurrentRow();
	
	abstract boolean isNull(int offset) throws Exception;
	
	abstract boolean getBoolean( int offset, int dataType) throws Exception;
	
	abstract byte[] getBytes( int offset, int dataType) throws Exception;
	
	abstract double getDouble( int offset, int dataType) throws Exception;
	
	abstract float getFloat( int offset, int dataType) throws Exception;
	
	abstract int getInt( int offset, int dataType) throws Exception;
	
	abstract long getLong( int offset, int dataType) throws Exception;
	
	abstract long getMoney( int offset, int dataType) throws Exception;
	
	abstract MutableNumeric getNumeric( int offset, int dataType) throws Exception;
	
	abstract Object getObject( int offset, int dataType) throws Exception;
	
	abstract String getString( int offset, int dataType) throws Exception;
	
	
	
	/**
	 * Get the status of the current page.
	 * @return true if the current row valid. false if deleted or updated data.
	 */
	boolean isValidPage(){
		return false;
	}
	
	abstract void scanObjectOffsets( int[] offsets, int dataTypes[] );
	
	abstract int getUsedSize();
	
	abstract long getNextPagePos();
	
	abstract void deleteRow(SSConnection con) throws SQLException;
}