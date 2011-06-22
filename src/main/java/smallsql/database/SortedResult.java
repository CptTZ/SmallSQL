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
 * SortedResult.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.database;

import smallsql.database.language.Language;

/**
 * Is used to implements the ORDER BY clause.
 * 
 * @author Volker Berlin
 */
final class SortedResult extends RowSource {

	final private Expressions orderBy;
    /**
     * The underlying RowSource that should be sorted.
     */
	final private RowSource rowSource;
    /**
     * Scroll pointer to the index.
     */
	private IndexScrollStatus scrollStatus;
    /**
     * The current row number. It is used for getRow().
     */
	private int row;
    /**
     * Added (inserted) rows if it is an updatable ResultSet.
     */
    private final LongList insertedRows = new LongList();
	private boolean useSetRowPosition;
    /**
     * The count of rows in the sorted index. This is the count without inserted rows.
     */
    private int sortedRowCount;
    /**
     * The rowOffset of the original RowSource (rowSource) that was read in the SortedResult.
     * This can be the last row of the original RowSource. But there can be also insert new row after it.
     */
    private long lastRowOffset;
    
	
	SortedResult(RowSource rowSource, Expressions orderBy){
		this.rowSource = rowSource;
		this.orderBy = orderBy;
	}

	
	final boolean isScrollable(){
		return true;
	}

	
	final void execute() throws Exception{
		rowSource.execute();
		Index index = new Index(false);	
        lastRowOffset = -1;
		while(rowSource.next()){
            lastRowOffset = rowSource.getRowPosition();
			index.addValues( lastRowOffset, orderBy);
            sortedRowCount++;
		}
		scrollStatus = index.createScrollStatus(orderBy);
		useSetRowPosition = false;
	}
	

    final boolean isBeforeFirst(){
        return row == 0;
    }
    
    
    final boolean isFirst(){
        return row == 1;
    }
    
    
    void beforeFirst() throws Exception {
		scrollStatus.reset();
		row = 0;
		useSetRowPosition = false;
	}
	

	boolean first() throws Exception {
		beforeFirst();
		return next();
	}
	

    boolean previous() throws Exception{
        if(useSetRowPosition) throw SmallSQLException.create(Language.ORDERBY_INTERNAL);
        if(currentInsertedRow() == 0){
            scrollStatus.afterLast();
        }
        row--;
        if(currentInsertedRow() >= 0){
            rowSource.setRowPosition( insertedRows.get( currentInsertedRow() ) );
            return true;
        }
        long rowPosition = scrollStatus.getRowOffset(false);
        if(rowPosition >= 0){
            rowSource.setRowPosition( rowPosition );
            return true;
        }else{
            rowSource.noRow();
            row = 0;
            return false;
        }
    }
	
	
	boolean next() throws Exception {
		if(useSetRowPosition) throw SmallSQLException.create(Language.ORDERBY_INTERNAL);
        if(currentInsertedRow() < 0){
    		long rowPosition = scrollStatus.getRowOffset(true);
    		if(rowPosition >= 0){
                row++;
    			rowSource.setRowPosition( rowPosition );
    			return true;
    		}
        }
        if(currentInsertedRow() < insertedRows.size()-1){
            row++;
            rowSource.setRowPosition( insertedRows.get( currentInsertedRow() ) );
            return true;
        }
        if(lastRowOffset >= 0){
            rowSource.setRowPosition( lastRowOffset );
        }else{
            rowSource.beforeFirst();
        }
        if(rowSource.next()){
            row++;
            lastRowOffset = rowSource.getRowPosition();
            insertedRows.add( lastRowOffset );
            return true;
        }
        rowSource.noRow();
        row = (getRowCount() > 0) ? getRowCount() + 1 : 0;
		return false;
	}
	
	
	boolean last() throws Exception{
		afterLast();
		return previous();
	}
	
	
    final boolean isLast() throws Exception{
        if(row == 0){
            return false;
        }
        if(row > getRowCount()){
            return false;
        }
        boolean isNext = next();
        previous();
        return !isNext;
    }
    
    
    final boolean isAfterLast(){
        int rowCount = getRowCount();
        return row > rowCount || rowCount == 0;
    }
    
    
	void afterLast() throws Exception{
        useSetRowPosition = false;
        if(sortedRowCount > 0){
            scrollStatus.afterLast();
            scrollStatus.getRowOffset(false); //previous position
        }else{
            rowSource.beforeFirst();
        }
        row = sortedRowCount;
        while(next()){
            // scroll to the end if there inserted rows
        }
	}
    
    
    boolean absolute(int newRow) throws Exception{
        if(newRow == 0) throw SmallSQLException.create(Language.ROW_0_ABSOLUTE);
        if(newRow > 0){
            beforeFirst();
            while(newRow-- > 0){
                if(!next()){
                    return false;
                }
            }
        }else{
            afterLast();
            while(newRow++ < 0){
                if(!previous()){
                    return false;
                }
            }
        }
        return true;
    }
    
    
    boolean relative(int rows) throws Exception{
        if(rows == 0) return (row != 0);
        if(rows > 0){
            while(rows-- > 0){
                if(!next()){
                    return false;
                }
            }
        }else{
            while(rows++ < 0){
                if(!previous()){
                    return false;
                }
            }
        }
        return true;
    }
    
    
	int getRow(){
		return row > getRowCount() ? 0 : row;
	}
	
	
	final long getRowPosition(){
		return rowSource.getRowPosition();
	}
	
	
	final void setRowPosition(long rowPosition) throws Exception{
		rowSource.setRowPosition(rowPosition);
		useSetRowPosition = true;
	}
	

	final boolean rowInserted(){
		return rowSource.rowInserted();
	}
	
	
	final boolean rowDeleted(){
		return rowSource.rowDeleted();
	}
	
	
	void nullRow() {
		rowSource.nullRow();
		row = 0;
		
	}
	

	void noRow() {
		rowSource.noRow();
		row = 0;
	}
    
    
    /**
     * @inheritDoc
     */
    boolean isExpressionsFromThisRowSource(Expressions columns){
        return rowSource.isExpressionsFromThisRowSource(columns);
    }
    
    
    /**
     * Get the current known row count. This is the sum of queried, sorted rows and inserted rows.
     */
    private final int getRowCount(){
        return sortedRowCount + insertedRows.size();
    }
    
    /**
     * Calculate the row position in the inserted rows. This is a pointer to insertedRows.
     * If the row pointer is not in the inserted rows then the value is negative.
     */
    private final int currentInsertedRow(){
        return row - sortedRowCount - 1;
    }

}
