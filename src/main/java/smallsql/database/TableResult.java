/* =============================================================
 * SmallSQL : a free Java DBMS library for the Java(tm) platform
 * =============================================================
 *
 * (C) Copyright 2004-2011, by Volker Berlin.
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
 * TableResult.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.database;

import java.sql.*;
import java.util.List;


final class TableResult extends TableViewResult{

    final private Table table;
    /** A List of rows that was inserted. The rows can be uncommited in memory or commited on harddisk.
     *  The WHERE condition does not need to be valid.
     */ 
    private List insertStorePages;
    /**
     * The filePos of the first own insert with insertRow(). On this rows the WHERE condition is not verify.
     */
    private long firstOwnInsert; 
    /**
     * The max fileOffset at open the ResultSet. Rows that are commited later are not not counted.
     */
    private long maxFileOffset;
    
	TableResult(Table table){
		this.table = table;
	}
	
    
	/**
	 * Is used for compile() of different Commands
	 * 
	 * @param con
	 * @return true if now initialize; false if already initialize
	 * @throws Exception
	 */
	@Override
    final boolean init( SSConnection con ) throws Exception{
		if(super.init(con)){
			Columns columns = table.columns;
			offsets     = new int[columns.size()];
			dataTypes   = new int[columns.size()];
			for(int i=0; i<columns.size(); i++){
				dataTypes[i] = columns.get(i).getDataType();
			}
			return true;
		}
		return false;
	}


	@Override
    final void execute() throws Exception{
		insertStorePages = table.getInserts(con);
		firstOwnInsert = 0x4000000000000000L | insertStorePages.size();
		maxFileOffset = table.raFile.size();
        beforeFirst();
	}

/*==============================================================================

	Methods for base class TableViewResult

==============================================================================*/

	@Override
    final TableView getTableView(){
		return table;
	}
    

    
	@Override
    final void deleteRow() throws SQLException{
		store.deleteRow(con); 
		store = new StoreNull(store.getNextPagePos());
	}
	

	/**
	 * {@inheritDoc}
	 */
	@Override
    final void updateRow(Expression[] updateValues) throws Exception{
		Columns tableColumns = table.columns;
		int count = tableColumns.size();
			
		StoreImpl newStore = table.getStoreTemp(con);
		
		// the write lock only prevent access from other connections 
		// but not access from other threads from the same connection
		// This can produce NPE if another thread commit pages of this thread
		synchronized(con.getMonitor()){
		    ((StoreImpl)this.store).createWriteLock();
		
    		for(int i=0; i<count; i++){
    			Expression src = updateValues[i];
    			if(src != null){
    			    //System.err.println(src.getInt()+" "+ ++xx);
    				newStore.writeExpression( src, tableColumns.get(i) );
    			}else{
    				copyValueInto( i, newStore );
    			}
    		}
    		((StoreImpl)this.store).updateFinsh(con, newStore);
		}
	}
    

	@Override
    final void insertRow(Expression[] updateValues) throws Exception{
		Columns tableColumns = table.columns;
		int count = tableColumns.size();
		
		// save the new values if there are new value for this table
		StoreImpl store = table.getStoreInsert(con);
		for(int i=0; i<count; i++){
			Column tableColumn = tableColumns.get(i);
			Expression src = updateValues[i];
			if(src == null) src = tableColumn.getDefaultValue(con);
			store.writeExpression( src, tableColumn );
						
		}
		store.writeFinsh( con );
		insertStorePages.add(store.getLink());
	}


/*==============================================================================

    Methods for Interface RowSource

==============================================================================*/
    private Store store = Store.NOROW;
    
    /** The row position in the file of the table.  */	
    private long filePos; 
    private int[] offsets;
    private int[] dataTypes;
    private int row;

    /** save the file offset after the last valid row (not deleted) */
    private long afterLastValidFilePos;
    
    /**
     * Move to the row in the filePos. A value of -1 for filePos is invalid at this call point.
     */
    final private boolean moveToRow() throws Exception{
    	if(filePos >= 0x4000000000000000L){
    		store = ((StorePageLink)insertStorePages.get( (int)(filePos & 0x3FFFFFFFFFFFFFFFL) )).getStore( table, con, lock);
    	}else{
    		store = (filePos < maxFileOffset) ? table.getStore( con, filePos, lock ) : null;
			if(store == null){
				if(insertStorePages.size() > 0){			
					filePos = 0x4000000000000000L;
					store = ((StorePageLink)insertStorePages.get( (int)(filePos & 0x3FFFFFFFFFFFFFFFL) )).getStore( table, con, lock);
				}
			}
    	}
		if(store != null){
			if(!store.isValidPage()){
				return false;
			}
			store.scanObjectOffsets( offsets, dataTypes );
			afterLastValidFilePos = store.getNextPagePos();
			return true;
		}else{
			filePos = -1;
			noRow();
			return false;
		}
    }
    
    
    /**
     * Move to the next valid row. A valid row is a normal row or an pointer to an updated row value.
     * A invalid row is a deleted row or an updated value that is reference by an update pointer. 
     */
    final private boolean moveToValidRow() throws Exception{
		while(filePos >= 0){
        	if(moveToRow())
        		return true;
			setNextFilePos();
    	}
        row = 0;
    	return false;
    }
    

	@Override
    final void beforeFirst(){
		filePos = 0;
		store = Store.NOROW;
		row = 0;
	}
    

	@Override
    final boolean first() throws Exception{
		filePos = table.getFirstPage();
		row = 1;
		return moveToValidRow();
	}
	
	
	/**
	 * A negative filePos means no more rows.<p>
	 * A value larger 0x4000000000000000L means a row that was inserted in this ResultSet.<p>
	 * A value of 0 means beforeFirst.<p>
	 * All other values are read position in the file.<p>
	 *
	 */
	final private void setNextFilePos(){
		if(filePos < 0) return; // end of rows
		if(store == Store.NOROW)
			 filePos = table.getFirstPage(); // can point at the end of file
		else
		if(filePos >= 0x4000000000000000L){
			filePos++;
			if((filePos & 0x3FFFFFFFFFFFFFFFL) >= insertStorePages.size()){
				filePos = -1;
				noRow();
			}
		}else
			filePos = store.getNextPagePos();
	}
    
    @Override
    final boolean next() throws Exception{
        if(filePos < 0) return false;
		setNextFilePos();
        row++;
        return moveToValidRow();
    }
    
    
	@Override
    final void afterLast(){
		filePos = -1;
		noRow();
	}
	
    
	@Override
    final int getRow(){
    	return row;
    }
	
    
	/**
	 * Get the position of the row in the file. This is equals to the rowOffset.
	 */
	@Override
    final long getRowPosition(){
		return filePos;
	}
	
	
	@Override
    final void setRowPosition(long rowPosition) throws Exception{
		filePos = rowPosition;
		if(filePos < 0 || !moveToRow()){
			store = new StoreNull(store.getNextPagePos());
		}
	}
	
	
	@Override
    final boolean rowInserted(){
		return filePos >= firstOwnInsert;
	}
	
	
	@Override
    final boolean rowDeleted(){
		// A StoreNull is created on setRowPosition on a deleted row
        // The instance Store.NULL is used for an empty outer join
		if(store instanceof StoreNull && store != Store.NULL){
            return true;
        }
        if(store instanceof StoreImpl &&
            ((StoreImpl)store).isRollback()){
            return true;
        }
        return false;
	}
	
	
	@Override
    final void nullRow(){
		row = 0;
    	store = Store.NULL;
    }
	

	@Override
    final void noRow(){
		row = 0;
		store = Store.NOROW;
	}
	

/*=======================================================================
 
 	Methods for Data Access
 
=======================================================================*/

	@Override
    final boolean isNull( int colIdx ) throws Exception{
        return store.isNull( offsets[colIdx] );
    }

	@Override
    final boolean getBoolean( int colIdx ) throws Exception{
        return store.getBoolean( offsets[colIdx], dataTypes[colIdx] );
    }

	@Override
    final int getInt( int colIdx ) throws Exception{
        return store.getInt( offsets[colIdx], dataTypes[colIdx] );
    }

	@Override
    final long getLong( int colIdx ) throws Exception{
        return store.getLong( offsets[colIdx], dataTypes[colIdx] );
    }

	@Override
    final float getFloat( int colIdx ) throws Exception{
        return store.getFloat( offsets[colIdx], dataTypes[colIdx] );
    }

	@Override
    final double getDouble( int colIdx ) throws Exception{
        return store.getDouble( offsets[colIdx], dataTypes[colIdx] );
    }

	@Override
    final long getMoney( int colIdx ) throws Exception{
        return store.getMoney( offsets[colIdx], dataTypes[colIdx] );
    }

	@Override
    final MutableNumeric getNumeric( int colIdx ) throws Exception{
        return store.getNumeric( offsets[colIdx], dataTypes[colIdx] );
    }

	@Override
    final Object getObject( int colIdx ) throws Exception{
        return store.getObject( offsets[colIdx], dataTypes[colIdx] );
    }

	@Override
    final String getString( int colIdx ) throws Exception{
        return store.getString( offsets[colIdx], dataTypes[colIdx] );
    }

	@Override
    final byte[] getBytes( int colIdx ) throws Exception{
        return store.getBytes( offsets[colIdx], dataTypes[colIdx] );
    }

	@Override
    final int getDataType( int colIdx ){
        return dataTypes[colIdx];
    }
    
    final private void copyValueInto( int colIdx, StoreImpl dst){
    	int offset = offsets[colIdx++];
    	int length = (colIdx < offsets.length ? offsets[colIdx] : store.getUsedSize()) - offset;
		dst.copyValueFrom( (StoreImpl)store, offset, length);
    }

    
}
