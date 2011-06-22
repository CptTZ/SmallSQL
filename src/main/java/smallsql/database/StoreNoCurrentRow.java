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
 * StoreNoCurrentRow.java
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
 * This store is used if the row pointer is before or after the rows.
 */
public class StoreNoCurrentRow extends Store {

	private SQLException noCurrentRow(){
		return SmallSQLException.create(Language.ROW_NOCURRENT);
	}


	boolean isNull(int offset) throws SQLException {
		throw noCurrentRow();
	}


	boolean getBoolean(int offset, int dataType) throws Exception {
		throw noCurrentRow();
	}


	byte[] getBytes(int offset, int dataType) throws Exception {
		throw noCurrentRow();
	}


	double getDouble(int offset, int dataType) throws Exception {
		throw noCurrentRow();
	}


	float getFloat(int offset, int dataType) throws Exception {
		throw noCurrentRow();
	}


	int getInt(int offset, int dataType) throws Exception {
		throw noCurrentRow();
	}


	long getLong(int offset, int dataType) throws Exception {
		throw noCurrentRow();
	}


	long getMoney(int offset, int dataType) throws Exception {
		throw noCurrentRow();
	}


	MutableNumeric getNumeric(int offset, int dataType) throws Exception {
		throw noCurrentRow();
	}


	Object getObject(int offset, int dataType) throws Exception {
		throw noCurrentRow();
	}


	String getString(int offset, int dataType) throws Exception {
		throw noCurrentRow();
	}



	void scanObjectOffsets(int[] offsets, int[] dataTypes) {
		// TODO Auto-generated method stub

	}


	int getUsedSize() {
		// TODO Auto-generated method stub
		return 0;
	}

	long getNextPagePos(){
		//TODO
		return -1;
	}
	
	void deleteRow(SSConnection con) throws SQLException{
		throw noCurrentRow();
	}
}
