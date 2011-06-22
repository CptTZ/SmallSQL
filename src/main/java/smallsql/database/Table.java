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
 * Table.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.database;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import smallsql.database.language.Language;

class Table extends TableView{
	
	private static final int INDEX = 1;

    final Database database;
    FileChannel raFile; // file handle of the table
	private Lobs lobs; // file handle of lob data for this table
    long firstPage; // offset of the first page

	final private HashMap locks = new HashMap();
	private SSConnection tabLockConnection; // if set then it is the Connection with a LOCK_TAB
	private int tabLockCount;
	/** if set then it is the Connection with a LOCK_WRITE_TAB */
	final private ArrayList locksInsert = new ArrayList(); // liste der LOCK_INSERT
	final private HashMap serializeConnections = new HashMap();
	final IndexDescriptions indexes;
	final ForeignKeys references;


	/**
	 * Constructor for read existing tables.
	 */
    Table( Database database, SSConnection con, String name, FileChannel raFile, long offset, int tableFormatVersion) throws Exception{
        super( name, new Columns() );
        this.database = database;
        this.raFile   = raFile;
		this.firstPage = offset;
		StoreImpl store = getStore(con, firstPage, SQLTokenizer.SELECT);
        if(store == null){
            throw SmallSQLException.create(Language.TABLE_FILE_INVALID, getFile(database));
        }
		int count = store.readInt();

		for(int i=0; i<count; i++){
			columns.add( store.readColumn(tableFormatVersion) );
		}
		indexes = new IndexDescriptions();
        references = new ForeignKeys();
		
		// read additional informations
		int type;
		while((type = store.readInt()) != 0){
			int offsetInPage = store.getCurrentOffsetInPage();
			int size = store.readInt();
			switch(type){
				case INDEX:
					indexes.add( IndexDescription.load( database, this, store) );
					break;
			}
			store.setCurrentOffsetInPage(offsetInPage + size);
		}
		
		firstPage = store.getNextPagePos();
    }
    

    /**
     * Constructor for creating of new tables.
     */
    Table(Database database, SSConnection con, String name, Columns columns, IndexDescriptions indexes, ForeignKeys foreignKeys) throws Exception{
        this(database, con, name, columns, null, indexes, foreignKeys);
    }
    
    /**
     * Constructor for alter an existing tables.
     */
    Table(Database database, SSConnection con, String name, Columns columns, IndexDescriptions existIndexes, IndexDescriptions newIndexes, ForeignKeys foreignKeys) throws Exception{
        super( name, columns );
        this.database = database;
        this.references = foreignKeys;
        newIndexes.create(con, database, this);
        if(existIndexes == null){
            this.indexes = newIndexes;
        }else{
            this.indexes = existIndexes;
            existIndexes.add(newIndexes);
        }
        
        write(con);
        for(int i=0; i<foreignKeys.size(); i++){
            ForeignKey foreignKey = foreignKeys.get(i);
            Table pkTable = (Table)database.getTableView(con, foreignKey.pkTable);
            pkTable.references.add(foreignKey);
        }
    }
    
    /**
     * Constructor for extends class Lobs.
     */
    Table(Database database, String name){
    	super( name, null);
    	this.database = database;
		indexes = null;
        references = null;
    }

	/**
	 * Drop the Table. This method is static that the file does not need to load and also corrupt files can be dropped.
	 */ 
    static void drop(Database database, String name) throws Exception{
        boolean ok = new File( Utils.createTableViewFileName( database, name ) ).delete();
        if(!ok) throw SmallSQLException.create(Language.TABLE_CANT_DROP, name);
    }
    
    
    /**
     * Drop a loaded table.
     *
     */
    void drop(SSConnection con) throws Exception{
		TableStorePage storePage = requestLock( con, SQLTokenizer.CREATE, -1 );
		if(storePage == null){
			throw SmallSQLException.create(Language.TABLE_CANT_DROP_LOCKED, name);
        }
		// remove the all commits that point to this table
		con.rollbackFile(raFile);
		close();
		if(lobs != null)
			lobs.drop(con);
		if(indexes != null)
			indexes.drop(database);
		boolean ok = getFile(database).delete();
		if(!ok) throw SmallSQLException.create(Language.TABLE_CANT_DROP, name);
    }
    

    /**
     * Closed the file handle that the object can be garbaged.
     */
    @Override
    void close() throws Exception{
        if(indexes != null)
            indexes.close();
        raFile.close();
        raFile = null;
        if( lobs != null ){
            lobs.close();
            lobs = null;
        }
    }


    private void write(SSConnection con) throws Exception{
        raFile = createFile( con, database );
        firstPage = 8;
        StoreImpl store = getStore( con, firstPage, SQLTokenizer.CREATE);
        int count = columns.size();
        store.writeInt( count );
        for(int i=0; i<count; i++){
            store.writeColumn(columns.get(i));
        }

		// write additional informations
		for(int i=0; i<indexes.size(); i++){
			IndexDescription indexDesc = indexes.get(i);
			store.writeInt( INDEX );
			int offsetStart = store.getCurrentOffsetInPage();
			store.setCurrentOffsetInPage( offsetStart + 4 ); // place holder for length
			
			// write the IndexDescription
			indexDesc.save(store);
			
			// write the length information
			int offsetEnd = store.getCurrentOffsetInPage();
			store.setCurrentOffsetInPage( offsetStart );
			store.writeInt( offsetEnd - offsetStart);
			store.setCurrentOffsetInPage( offsetEnd );
		}
		store.writeInt( 0 ); // no more additional informations
		
		store.writeFinsh(null); //The connection parameter is null because the table header is written immediately.
        firstPage = store.getNextPagePos();
    }
    

	@Override
    void writeMagic(FileChannel raFile) throws Exception{
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putInt(MAGIC_TABLE);
        buffer.putInt(TABLE_VIEW_VERSION);
        buffer.position(0);
        raFile.write(buffer);
	}
	

    /*StoreImpl getStoreCreate( SSConnection con, long filePos ) throws Exception{
        return StoreImpl.createStore( con, raFile, SQLTokenizer.CREATE, filePos );
    }*/

    StoreImpl getStore( SSConnection con, long filePos, int pageOperation ) throws Exception{
		TableStorePage storePage = requestLock( con, pageOperation, filePos );
        return StoreImpl.createStore( this, storePage, pageOperation, filePos );
    }

    
	StoreImpl getStore( TableStorePage storePage, int pageOperation ) throws Exception{
		// is used for not committed INSERT pages, a new lock is not needed
		return StoreImpl.recreateStore( this, storePage, pageOperation );
	}
	
    /*StoreImpl getStoreUpdate( SSConnection con, long filePos ) throws Exception{
        return StoreImpl.createStore( con, raFile, SQLTokenizer.UPDATE, filePos );
    }

    StoreImpl getStoreDelete( SSConnection con, long filePos ) throws Exception{
        return StoreImpl.createStore( con, raFile, SQLTokenizer.DELETE, filePos );
    }*/
	

    StoreImpl getStoreInsert( SSConnection con ) throws Exception{
		TableStorePage storePage = requestLock( con, SQLTokenizer.INSERT, -1 );
        return StoreImpl.createStore( this, storePage, SQLTokenizer.INSERT, -1 );
    }
    
    
    /**
     * Create a Store that is not invoke in a transaction for copy of data.
     */
	StoreImpl getStoreTemp( SSConnection con ) throws Exception{
		TableStorePage storePage = new TableStorePage( con, this, LOCK_NONE, -2);
		return StoreImpl.createStore( this, storePage, SQLTokenizer.INSERT, -2 );
	}
        

	StoreImpl getLobStore(SSConnection con, long filePos, int pageOperation) throws Exception{
		if(lobs == null){
			lobs = new Lobs( this );
		}
		return lobs.getStore( con, filePos, pageOperation );
	}
    

	
	/**
	 * Return the file offset of the first page with data after the table declaration.
	 * This is equals to the first row.
	 */
    final long getFirstPage(){
        return firstPage;
    }


    /**
     * Return a list of Links to not commited rows. The list include only the rows that are visible for 
     * the current isolation level.
     */
    List getInserts(SSConnection con){
		synchronized(locks){
			ArrayList inserts = new ArrayList();
			if(con.isolationLevel <= Connection.TRANSACTION_READ_UNCOMMITTED){
				for(int i=0; i<locksInsert.size(); i++){
					TableStorePageInsert lock = (TableStorePageInsert)locksInsert.get(i);
					inserts.add(lock.getLink());
				}
			}else{
				for(int i=0; i<locksInsert.size(); i++){
					TableStorePageInsert lock = (TableStorePageInsert)locksInsert.get(i);
					if(lock.con == con)
						inserts.add(lock.getLink());
				}
			}
			return inserts;
		}    	
    }
    
    
    /**
     * Request a page lock. If the request is valid then it return the StorePage. 
     * If the lock can not be created within 5 seconds then it throw an exception.
     * @param con The connection that request the lock
     * @param pageOperation The operation that should be perform
     * @param page The offset of the page
     * @return a valid StorePage
     * @throws Exception if a timeout occurs
     */
    final TableStorePage requestLock(SSConnection con, int pageOperation, long page) throws Exception{
    	synchronized(locks){
            if(raFile == null){
                throw SmallSQLException.create(Language.TABLE_MODIFIED, name);
            }
			long endTime = 0;
			while(true){
				TableStorePage storePage = requestLockImpl( con, pageOperation, page);
				if(storePage != null) 
					return storePage; // the normal case should be the fasted
				if(endTime == 0)
					endTime = System.currentTimeMillis() + 5000;
				long waitTime = endTime - System.currentTimeMillis();
				if(waitTime <= 0)
					throw SmallSQLException.create(Language.TABLE_DEADLOCK, name);
				locks.wait(waitTime);
			}
    	}
    }
    
    /**
     * Request a page lock. If the request is valid then it return the StorePage. 
     * In the other case it return null.
     * @param page The fileOffset or -1 for a new page
     * @throws SQLException 
     */
	final private TableStorePage requestLockImpl(SSConnection con, int pageOperation, long page) throws SQLException{
		synchronized(locks){
			if(tabLockConnection != null && tabLockConnection != con) return null;
			switch(con.isolationLevel){
				case Connection.TRANSACTION_SERIALIZABLE:
					serializeConnections.put( con, con);
					break;
			}
		
			switch(pageOperation){
				case SQLTokenizer.CREATE:{
						// first check if another connection has a lock before creating a table lock
						if(locks.size() > 0){
							Iterator values = locks.values().iterator();
							while(values.hasNext()){
								TableStorePage lock = (TableStorePage)values.next();
								if(lock.con != con) return null;
							}
						}
						for(int i=0; i<locksInsert.size(); i++){
							//the first StorePage in the linked list must be ever TableStorePageInsert
							TableStorePageInsert lock = (TableStorePageInsert)locksInsert.get(i);
							if(lock.con != con) return null;
						}
						if(serializeConnections.size() > 0){
							Iterator values = locks.values().iterator();
							while(values.hasNext()){
								TableStorePage lock = (TableStorePage)values.next();
								if(lock.con != con) return null;
							}
						}
						tabLockConnection = con;
						tabLockCount++;
						TableStorePage lock = new TableStorePage(con, this, LOCK_TAB, page);
						con.add(lock);
						return lock;
					}
                case SQLTokenizer.ALTER:{
                    // first check if there is any lock before creating a table lock
                    if(locks.size() > 0 || locksInsert.size() > 0){
                        return null;
                    }
                    if(serializeConnections.size() > 0){
                        Iterator values = locks.values().iterator();
                        while(values.hasNext()){
                            TableStorePage lock = (TableStorePage)values.next();
                            if(lock.con != con) return null;
                        }
                    }
                    tabLockConnection = con;
                    tabLockCount++;
                    TableStorePage lock = new TableStorePage(con, this, LOCK_TAB, page);
                    lock.rollback();
                    return lock;
                }
				case SQLTokenizer.INSERT:{
						// if there are more as one Connection with a serializable lock then an INSERT is not valid
						if(serializeConnections.size() > 1) return null;
						if(serializeConnections.size() == 1 && serializeConnections.get(con) == null) return null;
						TableStorePageInsert lock = new TableStorePageInsert(con, this, LOCK_INSERT);
						locksInsert.add( lock );
						con.add(lock);
						return lock;
					}
				case SQLTokenizer.SELECT:
				case SQLTokenizer.UPDATE:{
						Long pageKey = new Long(page); //TODO performance
						TableStorePage prevLock = null;
						TableStorePage lock = (TableStorePage)locks.get( pageKey );
						TableStorePage usableLock = null;
						while(lock != null){
							if(lock.con == con || 
							   con.isolationLevel <= Connection.TRANSACTION_READ_UNCOMMITTED){
							    usableLock = lock;
							} else {
							    if(lock.lockType == LOCK_WRITE){
							        return null; // write lock of another Connection
							    }
							}
							prevLock = lock;
							lock = lock.nextLock;
						}
						if(usableLock != null){
						    return usableLock;
						}
						lock = new TableStorePage( con, this, LOCK_NONE, page);
						if(con.isolationLevel >= Connection.TRANSACTION_REPEATABLE_READ || pageOperation == SQLTokenizer.UPDATE){
							lock.lockType = pageOperation == SQLTokenizer.UPDATE ? LOCK_WRITE : LOCK_READ;
							if(prevLock != null){
							    prevLock.nextLock = lock.nextLock;
							}else{
							    locks.put( pageKey, lock );
							}
							con.add(lock);
						}
						return lock;							
					}
				case SQLTokenizer.LONGVARBINARY:
					// is used for written BLOB and CLOB
					// the difference to INSERT is that page described the size of the byte buffer
					return new TableStorePage( con, this, LOCK_INSERT, -1);
				default:
					throw new Error("pageOperation:"+pageOperation);
			}
		}
	}
	
	
	/**
	 * Request a write lock for a page that is read. It add the resulting StorePage to the list of commits.
     * @throws SQLException
     *             if the connection was closed.
	 */
	TableStorePage requestWriteLock(SSConnection con, TableStorePage readlock) throws SQLException{
		if(readlock.lockType == LOCK_INSERT){
			TableStorePage lock = new TableStorePage( con, this, LOCK_INSERT, -1);
			readlock.nextLock = lock;
			con.add(lock);
			return lock;									
		}
		Long pageKey = new Long(readlock.fileOffset); //TODO performance
		TableStorePage prevLock = null;
		TableStorePage lock = (TableStorePage)locks.get( pageKey );
		while(lock != null){
			if(lock.con != con) return null; // there is already any lock from another connection, we can not start write
			if(lock.lockType < LOCK_WRITE){
				// if there is only a read lock we can transfer it
				// this is required for rollback to a savepoint
				lock.lockType = LOCK_WRITE;
				return lock;
			}
			prevLock = lock;
			lock = lock.nextLock;
		}
		lock = new TableStorePage( con, this, LOCK_WRITE, readlock.fileOffset);
		if(prevLock != null){
		    prevLock.nextLock = lock;
		} else {
		    locks.put( pageKey, lock );
		}
		con.add(lock);
		return lock;									
	}
	
	
	/**
	 * Remove the lock from this table.
	 */
	void freeLock(TableStorePage storePage){
		final int lockType = storePage.lockType;
		final long fileOffset = storePage.fileOffset;
		synchronized(locks){
			try{
				TableStorePage lock;
				TableStorePage prev;
				switch(lockType){
					case LOCK_INSERT:
						for(int i=0; i<locksInsert.size(); i++){
							prev = lock = (TableStorePage)locksInsert.get(i);
							while(lock != null){
								if(lock == storePage){
									//remove lock
									if(lock == prev){
										if(lock.nextLock == null){
											// the first lock is the only lock in the list
											locksInsert.remove(i--);
										}else{
											// only the first lock of the list is remove
											locksInsert.set( i, lock.nextLock );
										}
									}else{
										// a lock in the mid or end is removed
										prev.nextLock = lock.nextLock;
									}
									return;
								}
								prev = lock;
								lock = lock.nextLock;
							}
						}
						break;
					case LOCK_READ:
					case LOCK_WRITE:
						Long pageKey = new Long(fileOffset); //TODO performance
						lock = (TableStorePage)locks.get( pageKey );
						prev = lock;
						while(lock != null){
							if(lock == storePage){
								//lock entfernen
								if(lock == prev){
									if(lock.nextLock == null){
										// erste und einzige Lock in Liste
										locks.remove(pageKey);
									}else{
										// the first lock in the list is removed
										locks.put( pageKey, lock.nextLock );
									}
								}else{
									// a lock in the middle or end of the list is removed
									prev.nextLock = lock.nextLock;
								}
								return;
							}
							prev = lock;
							lock = lock.nextLock;
						}
						// a run through can occur if a lock was step high and the type does not compare
						break;
					case LOCK_TAB:
						assert storePage.con == tabLockConnection : "Internal Error with TabLock";
						if(--tabLockCount == 0) tabLockConnection = null;
						break;
					default:
						throw new Error();
				}
			}finally{
				locks.notifyAll();
			}
		}
	}

}

