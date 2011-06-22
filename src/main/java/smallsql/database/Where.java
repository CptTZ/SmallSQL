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
 * Where.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 14.08.2004
 */
package smallsql.database;


class Where extends RowSource {
	
	final private RowSource rowSource;
	final private Expression where;
	private int row = 0;
	private boolean isCurrentRow;
	
	Where(RowSource rowSource, Expression where){
		this.rowSource = rowSource;
		this.where = where;
	}
	
	RowSource getFrom(){
		return rowSource;
	}
	
	/**
	 * Verify if the valid row of the underlying RowSource (Variable join)
	 * is valid for the current ResultSet.
	 * @return
	 */
	final private boolean isValidRow() throws Exception{
		return where == null || rowSource.rowInserted() || where.getBoolean();
	}
	

	final boolean isScrollable() {
		return rowSource.isScrollable();
	}
	

	final boolean isBeforeFirst(){
		return row == 0;
	}
	
	
	final boolean isFirst(){
		return row == 1 && isCurrentRow;
	}
	
	
	final boolean isLast() throws Exception{
		if(!isCurrentRow) return false;
		long rowPos = rowSource.getRowPosition();
		boolean isNext = next();
		rowSource.setRowPosition(rowPos);
		return !isNext;
	}
	
	
	final boolean isAfterLast(){
		return row > 0 && !isCurrentRow;
	}
	
	
	final void beforeFirst() throws Exception {
		rowSource.beforeFirst();
		row = 0;
	}


	final boolean first() throws Exception {
		isCurrentRow = rowSource.first();
		while(isCurrentRow && !isValidRow()){
			isCurrentRow = rowSource.next();
		}
		row = 1;
		return isCurrentRow;
	}
	
	
	final boolean previous() throws Exception {
        boolean oldIsCurrentRow = isCurrentRow;
		do{
			isCurrentRow = rowSource.previous();
		}while(isCurrentRow && !isValidRow());
		if(oldIsCurrentRow || isCurrentRow) row--;
		return isCurrentRow;
	}


	final boolean next() throws Exception {
        boolean oldIsCurrentRow = isCurrentRow;
		do{
			isCurrentRow = rowSource.next();
		}while(isCurrentRow && !isValidRow());
		if(oldIsCurrentRow || isCurrentRow) row++;
		return isCurrentRow;
	}


	final boolean last() throws Exception{
		while(next()){/* scroll after the end */}
		return previous();
	}


	final void afterLast() throws Exception {
		while(next()){/* scroll after the end */}
	}
	
	
	final int getRow() throws Exception {
		return isCurrentRow ? row : 0;
	}


	final long getRowPosition() {
		return rowSource.getRowPosition();
	}


	final void setRowPosition(long rowPosition) throws Exception {
		rowSource.setRowPosition(rowPosition);
	}


	final void nullRow() {
		rowSource.nullRow();
		row = 0;
	}


	final void noRow() {
		rowSource.noRow();
		row = 0;
	}


	final boolean rowInserted() {
		return rowSource.rowInserted();
	}


	final boolean rowDeleted() {
		return rowSource.rowDeleted();
	}
	
	
	final void execute() throws Exception{
		rowSource.execute();
	}


    /**
     * @inheritDoc
     */
    boolean isExpressionsFromThisRowSource(Expressions columns){
        return rowSource.isExpressionsFromThisRowSource(columns);
    }
}
