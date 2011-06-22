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
 * TableView.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 05.06.2004
 */
package smallsql.database;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.*;

import smallsql.database.language.Language;


/**
 * @author Volker Berlin
 */
abstract class TableView {

	static final int MAGIC_TABLE = 'S' << 24 | 'Q' << 16 | 'L' << 8 | 'T';
	static final int MAGIC_VIEW  = 'S' << 24 | 'Q' << 16 | 'L' << 8 | 'V';
	static final int TABLE_VIEW_VERSION = 2;
	static final int TABLE_VIEW_OLD_VERSION = 1;
	
	final String name;
	final Columns columns;

	/** 
	 * Mark the last change on the structure of the Table or View.
	 * If this value change then PreparedStatements need to recompile.
	 */
	private long timestamp = System.currentTimeMillis();
	
	static final int LOCK_NONE   = 0; // read on READ_COMMITED and READ_UNCOMMITED
	static final int LOCK_INSERT = 1; // prevent only LOCK_TAB
	static final int LOCK_READ   = 2; // occur on read and prevent a write of data, it can occur more as one LOCK_READ per page
	static final int LOCK_WRITE  = 3; // occur on write and prevent every other access to the data, it is only one LOCK_WRITE per page possible
	static final int LOCK_TAB    = 4; // lock the total table


	TableView(String name, Columns columns){
		this.name = name;
		this.columns = columns;
	}
	
	/**
	 * Load a Table or View object. 
	 */
	static TableView load(SSConnection con, Database database, String name) throws SQLException{
	    FileChannel raFile = null;
		try{
			String fileName = Utils.createTableViewFileName( database, name );
			File file = new File( fileName );
			if(!file.exists())
				throw SmallSQLException.create(Language.TABLE_OR_VIEW_MISSING, name);
			raFile = Utils.openRaFile( file, database.isReadOnly() );
			ByteBuffer buffer = ByteBuffer.allocate(8);
			raFile.read(buffer);
			buffer.position(0);
			int magic   = buffer.getInt();
			int version = buffer.getInt();
			switch(magic){
				case MAGIC_TABLE:
				case MAGIC_VIEW:
						break;
				default:
					throw SmallSQLException.create(Language.TABLE_OR_VIEW_FILE_INVALID, fileName);
			}
			if(version > TABLE_VIEW_VERSION)
				throw SmallSQLException.create(Language.FILE_TOONEW, new Object[] { new Integer(version), fileName });
			if(version < TABLE_VIEW_OLD_VERSION)
				throw SmallSQLException.create(Language.FILE_TOOOLD, new Object[] { new Integer(version), fileName });
			if(magic == MAGIC_TABLE)
				return new Table( database, con, name, raFile, raFile.position(), version);
				return new View ( con, name, raFile, raFile.position());
		}catch(Throwable e){
			if(raFile != null)
				try{
					raFile.close();
				}catch(Exception e2){
					DriverManager.println(e2.toString());
				}
			throw SmallSQLException.createFromException(e);
		}
	}
	
	
	/**
	 * Get a file object for the current table or view. This is independent
	 * if it exists or not.
	 * @param database The database that the table or view include
	 * @return a file handle, never null
	 */
	File getFile(Database database){
		return new File( Utils.createTableViewFileName( database, name ) );
	}
	

	/**
	 * Create an empty table or view file that only include the signature.
	 * @param database The database that the table or view should include.
	 * @return A file handle
	 * @throws Exception if any error occur like
	 * <li>file exist already
	 * <li>SecurityException
	 */
	FileChannel createFile(SSConnection con, Database database) throws Exception{
	    if( database.isReadOnly() ){
	        throw SmallSQLException.create(Language.DB_READONLY);
	    }
		File file = getFile( database );
		boolean ok = file.createNewFile();
		if(!ok) throw SmallSQLException.create(Language.TABLE_EXISTENT, name);
		FileChannel raFile = Utils.openRaFile( file, database.isReadOnly() );
		con.add(new CreateFile(file, raFile, con, database));
		writeMagic(raFile);
		return raFile;
	}
	
	abstract void writeMagic(FileChannel raFile) throws Exception;

	
	String getName(){
		return name;
	}
    

	long getTimestamp(){
		return timestamp;
	}
	

	/**
	 * Returns the index of a column name. The first column has the index 0.
	 */
	final int findColumnIdx(String columnName){
		// FIXME switch to a tree search on performance reason
		for(int i=0; i<columns.size(); i++){
			if( columns.get(i).getName().equalsIgnoreCase(columnName) ) return i;
		}
		return -1;
	}


	/**
	 * Returns the Column of a column name.
	 */
	final Column findColumn(String columnName){
		for(int i=0; i<columns.size(); i++){
			Column column = columns.get(i);
			if( column.getName().equalsIgnoreCase(columnName) ) return column;
		}
		return null;
	}


	
	/**
	 * Close it and free all resources.
	 */
	void close() throws Exception{/* in this abstract class is nothing to free */}

}


