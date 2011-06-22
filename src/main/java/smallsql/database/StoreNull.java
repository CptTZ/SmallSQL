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
 * StoreNull.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.database;

import java.sql.*;
import smallsql.database.language.Language;

/*
 * @author Volker Berlin
 * 
 * This class is a implementation of the STore that returns only null. 
 * This is used for OUTER JOIN to return null values for tables with no
 * row position.
 *
 */
class StoreNull extends Store {

	private final long nextPagePos;
	
	
	StoreNull(){
		this(-1);
	}
	
	
	StoreNull(long nextPos){
		nextPagePos = nextPos;
	}
	
	
	final boolean isNull(int offset) {
		return true;
	}

	final boolean getBoolean(int offset, int dataType) throws Exception {
		return false;
	}

	final byte[] getBytes(int offset, int dataType) throws Exception {
		return null;
	}

	
	final double getDouble(int offset, int dataType) throws Exception {
		return 0;
	}


	final float getFloat(int offset, int dataType) throws Exception {
		return 0;
	}


	final int getInt(int offset, int dataType) throws Exception {
		return 0;
	}

	
	final long getLong(int offset, int dataType) throws Exception {
		return 0;
	}


	final long getMoney(int offset, int dataType) throws Exception {
		return 0;
	}


	final MutableNumeric getNumeric(int offset, int dataType) throws Exception {
		return null;
	}


	final Object getObject(int offset, int dataType) throws Exception {
		return null;
	}


	final String getString(int offset, int dataType) throws Exception {
		return null;
	}



	final void scanObjectOffsets(int[] offsets, int[] dataTypes) {/* there is nothing to scan */}


	final int getUsedSize() {
		return 0;
	}

	final long getNextPagePos(){
		return nextPagePos;
	}
	
	final void deleteRow(SSConnection con) throws SQLException{
		if(nextPagePos >= 0){
			throw SmallSQLException.create(Language.ROW_DELETED);
		}
		//TODO
		throw new Error();
	}
}
