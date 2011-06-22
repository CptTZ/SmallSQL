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
 * UnionAll.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 26.06.2004
 */
package smallsql.database;

import smallsql.database.language.Language;

/**
 * @author Volker Berlin
 */
final class UnionAll extends DataSource {

	private final DataSources dataSources = new DataSources();
	private int dataSourceIdx;
	private DataSource currentDS;
	private int row;
	
	
	void addDataSource(DataSource ds){
		dataSources.add(ds);
		currentDS = dataSources.get(0);
	}
	
	
/*=======================================================================
 
 	Methods for interface DataSource
 
=======================================================================*/
	
	boolean init(SSConnection con) throws Exception{
		boolean result = false;
		int colCount = -1;
		for(int i=0; i<dataSources.size(); i++){
			DataSource ds = dataSources.get(i);
			result |= ds.init(con);
			int nextColCount = ds.getTableView().columns.size();
			if(colCount == -1)
				colCount = nextColCount;
			else
				if(colCount != nextColCount)
					throw SmallSQLException.create(Language.UNION_DIFFERENT_COLS, new Object[] { new Integer(colCount), new Integer(nextColCount)});
		}	
		return result;
	}
	
	
	final boolean isNull(int colIdx) throws Exception {
		return currentDS.isNull(colIdx);
	}


	final boolean getBoolean(int colIdx) throws Exception {
		return currentDS.getBoolean(colIdx);
	}


	final int getInt(int colIdx) throws Exception {
		return currentDS.getInt(colIdx);
	}


	final long getLong(int colIdx) throws Exception {
		return currentDS.getLong(colIdx);
	}


	final float getFloat(int colIdx) throws Exception {
		return currentDS.getFloat(colIdx);
	}


	final double getDouble(int colIdx) throws Exception {
		return currentDS.getDouble(colIdx);
	}


	final long getMoney(int colIdx) throws Exception {
		return currentDS.getMoney(colIdx);
	}


	final MutableNumeric getNumeric(int colIdx) throws Exception {
		return currentDS.getNumeric(colIdx);
	}


	final Object getObject(int colIdx) throws Exception {
		return currentDS.getObject(colIdx);
	}


	final String getString(int colIdx) throws Exception {
		return currentDS.getString(colIdx);
	}


	final byte[] getBytes(int colIdx) throws Exception {
		return currentDS.getBytes(colIdx);
	}


	final int getDataType(int colIdx) {
		return currentDS.getDataType(colIdx);
	}
	

	TableView getTableView(){
		return currentDS.getTableView();
	}
	
	

/*=======================================================================
 
	Methods for interface RowSource
 
=======================================================================*/
	
	final boolean isScrollable(){
		return false; //TODO performance, can implement it if all datasources implement it
	}

	
	final void beforeFirst() throws Exception {
		dataSourceIdx = 0;
		currentDS = dataSources.get(0);
		currentDS.beforeFirst();
		row = 0;
	}


	final boolean first() throws Exception {
		dataSourceIdx = 0;
		currentDS = dataSources.get(0);
		boolean b = currentDS.first();
		row = b ? 1 : 0;
		return b;
	}


	final boolean next() throws Exception {
		boolean n = currentDS.next();
		row++;
		if(n) return true;
		while(dataSources.size() > dataSourceIdx+1){
			currentDS = dataSources.get(++dataSourceIdx);
			currentDS.beforeFirst();
			n = currentDS.next();
			if(n) return true;
		}
		row = 0;
		return false;
	}


	final void afterLast() throws Exception {
		dataSourceIdx = dataSources.size()-1;
		currentDS = dataSources.get(dataSourceIdx);
		currentDS.afterLast();
		row = 0;
	}


	final int getRow() throws Exception {
		return row;
	}
	

	private final int getBitCount(){
		int size = dataSources.size();
		int bitCount = 0;
		while(size>0){
			bitCount++;
			size >>= 1;
		}
		return bitCount;
	}

	
	final long getRowPosition() {
		int bitCount = getBitCount();
		return dataSourceIdx | currentDS.getRowPosition() << bitCount;
	}


	final void setRowPosition(long rowPosition) throws Exception {
		int bitCount = getBitCount();
		int mask = 0xFFFFFFFF >>> (32 - bitCount);
		dataSourceIdx = (int)rowPosition & mask;
		currentDS = dataSources.get(dataSourceIdx);
		currentDS.setRowPosition( rowPosition >> bitCount );
		//getRow() is only unse on the top level RowSource, setRowPosition is not used on the top level RowSource
		//thats we not set row here
	}


	final boolean rowInserted(){
		return currentDS.rowInserted();
	}
	
	
	final boolean rowDeleted(){
		return currentDS.rowDeleted();
	}
	
	
	final void nullRow() {
		currentDS.nullRow();
		row = 0;
	}


	final void noRow() {
		currentDS.noRow();
		row = 0;
	}

	
	final void execute() throws Exception{
		for(int i=0; i<dataSources.size(); i++){
			dataSources.get(i).execute();			
		}
	}

}
