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
 * Database.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.database;

import java.util.*;
import java.io.*;
import java.nio.channels.FileChannel;
import java.sql.*;
import smallsql.database.language.Language;

/**
 * There are only one instance of this class per database. It will be share between all connections to this database and
 * all threads. That the access must be thread safe.
 * 
 * Here are save mainly table definitions and locks.
 */
final class Database{

    static private HashMap databases = new HashMap();

    private final TableViewMap tableViews = new TableViewMap();
    private final String name;
    private final boolean readonly;
	private final File directory;
	private final FileChannel master;
	private final WeakHashMap connections = new WeakHashMap();


	/**
	 * Get a instance of the Database Class. If the Database with the given name is not open 
	 * then it will be open.
	 * @param name 	   the name of the database
	 * @param con 	   a reference holder to this database, if all connection close that have a reference 
	 * 	               then the database can be unload.
	 * @param create   if the database not exist then create it
	 */
    static Database getDatabase(String name, SSConnection con, boolean create) throws SQLException{
        if(name == null){
            return null;
        }
        if(name.startsWith("file:")){
            name = name.substring(5);
        }
        File file;
        try{
            file = new File(name).getCanonicalFile();
        }catch(Throwable th){
            throw SmallSQLException.createFromException( th );
        }
        String dbKey = file.getName() + ";readonly=" + con.isReadOnly();
        synchronized(databases){
            Database db = (Database)databases.get(dbKey);
            if(db == null){
                if(create && !file.isDirectory()){
                    CommandCreateDatabase command = new CommandCreateDatabase(con.log, name);
                    command.execute(con, null);
                }
                db = new Database( name, file, con.isReadOnly() );
                databases.put(dbKey, db);
            }
            db.connections.put(con, null);
            return db;
        }
    }
    
    
    private static Database getDatabase(SSConnection con, String name) throws SQLException{
		return name == null ?
					con.getDatabase(false) :
					getDatabase( name, con, false );
    }
    

    /**
     * Create a instance of a Database
     * @param name is used for getCatalog()
     * @param canonicalFile the directory that is already canonical
     * @param readonly open database in read only mode
     * @throws SQLException If can't open
     */
    private Database( String name, File canonicalFile, boolean readonly ) throws SQLException{
        try{
	        this.name = name;
	        this.readonly = readonly;
			directory = canonicalFile;
			if(!directory.isDirectory()){
                throw SmallSQLException.create(Language.DB_NONEXISTENT, name);
            }
			File file = new File( directory, Utils.MASTER_FILENAME);
			if(!file.exists())
				throw SmallSQLException.create(Language.DB_NOT_DIRECTORY, name);
			master = Utils.openRaFile( file, readonly );
        }catch(Exception e){
        	throw SmallSQLException.createFromException(e);
        }
    }

    String getName(){
        return name;
    }
    
	boolean isReadOnly(){
	    return readonly;
	}
	

	/**
	 * Remove a connection from this database.
	 */
	static final void closeConnection(SSConnection con) throws SQLException{
		synchronized(databases){
			Iterator iterator = databases.values().iterator();
			while(iterator.hasNext()){
				Database database = (Database)iterator.next();
				WeakHashMap connections = database.connections;
				connections.remove(con);
				if(connections.size() == 0){
					try {
						iterator.remove();
						database.close();
					} catch (Exception e) {
						throw SmallSQLException.createFromException(e);
					}
				}
			}
		}
	}
	

	/**
	 * Close all tables and views of this Database.
	 */
	private final void close() throws Exception{
		synchronized(tableViews){
			Iterator iterator = tableViews.values().iterator();
			while(iterator.hasNext()){
				TableView tableView = (TableView)iterator.next();
				tableView.close();
				iterator.remove();
			}
		}
		master.close();
	}
	
    static TableView getTableView(SSConnection con, String catalog, String tableName) throws SQLException{
    	return getDatabase( con, catalog).getTableView( con, tableName);
    }

    
    /**
     * Return a TableView object. If the TableView object is not loaded then it load it.
     * @param con
     * @param tableName
     * @return ever a valid TableView object and never null.
     * @throws SQLException if the table or view does not exists
     */
    TableView getTableView(SSConnection con, String tableName) throws SQLException{
        synchronized(tableViews){
            TableView tableView = tableViews.get(tableName);
            if(tableView == null){
                // FIXME it should block only one table and not all tables, loading of the table should outside of the global synchronized
                tableView = TableView.load(con, this, tableName);
                tableViews.put( tableName, tableView);
            }
            return tableView;
        }
    }
    

	static void dropTable(SSConnection con, String catalog, String tableName) throws Exception{
		getDatabase( con, catalog).dropTable( con, tableName);
	}
	

    void dropTable(SSConnection con, String tableName) throws Exception{
        synchronized(tableViews){
            Table table = (Table)tableViews.get( tableName );
            if(table != null){
				tableViews.remove( tableName );
                table.drop(con);
            }else{
            	Table.drop( this, tableName );
            }
        }
    }
    
    
    /**
     * Remove a table or view from the cache of open objects.
     * @param tableViewName the name of the object
     */
    void removeTableView(String tableViewName){
        synchronized(tableViews){
            tableViews.remove( tableViewName );
        }
    }
    
    
    void replaceTable( Table oldTable, Table newTable) throws Exception{
        synchronized(tableViews){
            tableViews.remove( oldTable.name );
            tableViews.remove( newTable.name );
            oldTable.close();
            newTable.close();
            File oldFile = oldTable.getFile(this);
            File newFile = newTable.getFile(this);
            File tmpFile = new File(Utils.createTableViewFileName( this, "#" + System.currentTimeMillis() + this.hashCode() ));
            if( !oldFile.renameTo(tmpFile) ){
                throw SmallSQLException.create(Language.TABLE_CANT_RENAME, oldTable.name);
            }
            if( !newFile.renameTo(oldFile) ){
                tmpFile.renameTo(oldFile); //restore the old table
                throw SmallSQLException.create(Language.TABLE_CANT_RENAME, oldTable.name);
            }
            tmpFile.delete();
        }
    }
    

	static void dropView(SSConnection con, String catalog, String tableName) throws Exception{
		getDatabase( con, catalog).dropView(tableName);
	}
	

	void dropView(String viewName) throws Exception{
		synchronized(tableViews){
			Object view = tableViews.remove( viewName );
			if(view != null && !(view instanceof View))
				throw SmallSQLException.create(Language.VIEWDROP_NOT_VIEW, viewName);
			
			View.drop( this, viewName );
		}
	}
    
    
    private void checkForeignKeys( SSConnection con, ForeignKeys foreignKeys ) throws SQLException{
        for(int i=0; i<foreignKeys.size(); i++){
            ForeignKey foreignKey = foreignKeys.get(i);
            TableView pkTable = getTableView(con, foreignKey.pkTable);
            if(!(pkTable instanceof Table)){
                throw SmallSQLException.create(Language.FK_NOT_TABLE, foreignKey.pkTable);
            }
        }
    }
    

    /**
     * @param con current Connections
     * @param name the name of the new Table
     * @param columns the column descriptions of the table
     * @param indexes the indexes of the new table
     * @param foreignKeys 
     * @throws Exception
     */
	void createTable(SSConnection con, String name, Columns columns, IndexDescriptions indexes, ForeignKeys foreignKeys) throws Exception{
        checkForeignKeys( con, foreignKeys );
        // createFile() can run only one Thread success (it is atomic)
        // Thats the create of the Table does not need in the Synchronized.
        Table table = new Table( this, con, name, columns, indexes, foreignKeys);
        synchronized(tableViews){
            tableViews.put( name, table);
        }
    }


    /**
     * It is used to create temp Table for ALTER TABLE and co.
     */
    Table createTable(SSConnection con, String tableName, Columns columns, IndexDescriptions oldIndexes, IndexDescriptions newIndexes, ForeignKeys foreignKeys) throws Exception{
        checkForeignKeys( con, foreignKeys );
        Table table = new Table( this, con, tableName, columns, oldIndexes, newIndexes, foreignKeys);
        synchronized(tableViews){
            tableViews.put( tableName, table);
        }
        return table;
    }
    
    
	void createView(SSConnection con, String viewName, String sql) throws Exception{
		// createFile() can run only one Thread success (it is atomic)
		// Thats the create of the View does not need in the Synchronized.
		new View( this, con, viewName, sql);
	}


    /**
     * Create a list of all available Databases from the point of the current 
     * Database or current working directory
     * @param database - current database
     * @return
     */
    static Object[][] getCatalogs(Database database){
    	List catalogs = new ArrayList();
    	File baseDir = (database != null) ?
    					database.directory.getParentFile() :
						new File(".");
		File dirs[] = baseDir.listFiles();
		if(dirs != null)
			for(int i=0; i<dirs.length; i++){
				if(dirs[i].isDirectory()){
					if(new File(dirs[i], Utils.MASTER_FILENAME).exists()){
						Object[] catalog = new Object[1];
						catalog[0] = dirs[i].getPath();
						catalogs.add(catalog);
					}
				}
			}
		Object[][] result = new Object[catalogs.size()][];
		catalogs.toArray(result);
		return result;
    }
	
    
	Strings getTables(String tablePattern){
		Strings list = new Strings();
		File dirs[] = directory.listFiles();    
		if(dirs != null)
			if(tablePattern == null) tablePattern = "%"; 
			tablePattern += Utils.TABLE_VIEW_EXTENTION;
			for(int i=0; i<dirs.length; i++){
				String name = dirs[i].getName();
				if(Utils.like(name, tablePattern)){
					list.add(name.substring( 0, name.length()-Utils.TABLE_VIEW_EXTENTION.length() ));
				}
			}
    	return list;
    }
	
    
    Object[][] getColumns( SSConnection con, String tablePattern, String colPattern) throws Exception{
    	List rows = new ArrayList();
		Strings tables = getTables(tablePattern);
    	for(int i=0; i<tables.size(); i++){
    		String tableName = tables.get(i);
			try{
	    		TableView tab = getTableView( con, tableName);
	    		Columns cols = tab.columns;
	    		for(int c=0; c<cols.size(); c++){
	    			Column col = cols.get(c);
					Object[] row = new Object[18];
					row[0] = getName(); 			//TABLE_CAT
								   					//TABLE_SCHEM
					row[2] = tableName;				//TABLE_NAME	
					row[3] = col.getName();			//COLUMN_NAME	
					row[4] = Utils.getShort( SQLTokenizer.getSQLDataType( col.getDataType() )); //DATA_TYPE  
					row[5] = SQLTokenizer.getKeyWord( col.getDataType() );	//TYPE_NAME
					row[6] = Utils.getInteger(col.getColumnSize());//COLUMN_SIZE
													//BUFFER_LENGTH
					row[8] = Utils.getInteger(col.getScale());//DECIMAL_DIGITS
					row[9] = Utils.getInteger(10);		//NUM_PREC_RADIX
					row[10]= Utils.getInteger(col.isNullable() ? DatabaseMetaData.columnNullable : DatabaseMetaData.columnNoNulls); //NULLABLE
													//REMARKS
					row[12]= col.getDefaultDefinition(); //COLUMN_DEF
													//SQL_DATA_TYPE
													//SQL_DATETIME_SUB
					row[15]= row[6];				//CHAR_OCTET_LENGTH
					row[16]= Utils.getInteger(i); 	//ORDINAL_POSITION		
					row[17]= col.isNullable() ? "YES" : "NO"; //IS_NULLABLE					
					rows.add(row);
	    		}
			}catch(Exception e){
				//invalid Tables and View will not show 
			}
    	}
		Object[][] result = new Object[rows.size()][];
		rows.toArray(result);
		return result;
    }
	
	
	Object[][] getReferenceKeys(SSConnection con, String pkTable, String fkTable) throws SQLException{
		List rows = new ArrayList();
		Strings tables = (pkTable != null) ? getTables(pkTable) : getTables(fkTable);
		for(int t=0; t<tables.size(); t++){
    		String tableName = tables.get(t);
    		TableView tab = getTableView( con, tableName);
			if(!(tab instanceof Table)) continue;
			ForeignKeys references = ((Table)tab).references;
			for(int i=0; i<references.size(); i++){
				ForeignKey foreignKey = references.get(i);
				IndexDescription pk = foreignKey.pk;
				IndexDescription fk = foreignKey.fk;
				if((pkTable == null || pkTable.equals(foreignKey.pkTable)) &&
				   (fkTable == null || fkTable.equals(foreignKey.fkTable))){
					Strings columnsPk = pk.getColumns();
					Strings columnsFk = fk.getColumns();
					for(int c=0; c<columnsPk.size(); c++){
						Object[] row = new Object[14];
						row[0] = getName();				//PKTABLE_CAT
														//PKTABLE_SCHEM
						row[2] = foreignKey.pkTable;	//PKTABLE_NAME
						row[3] = columnsPk.get(c);		//PKCOLUMN_NAME
						row[4] = getName();				//FKTABLE_CAT
														//FKTABLE_SCHEM
						row[6] = foreignKey.fkTable;	//FKTABLE_NAME
						row[7] = columnsFk.get(c);		//FKCOLUMN_NAME
						row[8] = Utils.getShort(c+1);	//KEY_SEQ
						row[9] = Utils.getShort(foreignKey.updateRule);//UPDATE_RULE
						row[10]= Utils.getShort(foreignKey.deleteRule); //DELETE_RULE
						row[11]= fk.getName();	//FK_NAME
						row[12]= pk.getName();	//PK_NAME
						row[13]= Utils.getShort(DatabaseMetaData.importedKeyNotDeferrable); //DEFERRABILITY
						rows.add(row);
					}
				}
			}
		}
		Object[][] result = new Object[rows.size()][];
		rows.toArray(result);
		return result;		
	}
	
	
	Object[][] getBestRowIdentifier(SSConnection con, String table) throws SQLException{
		List rows = new ArrayList();
		Strings tables = getTables(table);
		for(int t=0; t<tables.size(); t++){
    		String tableName = tables.get(t);
    		TableView tab = getTableView( con, tableName);
			if(!(tab instanceof Table)) continue;
			IndexDescriptions indexes = ((Table)tab).indexes;
			for(int i=0; i<indexes.size(); i++){
				IndexDescription index = indexes.get(i);
				if(index.isUnique()){
					Strings columns = index.getColumns();
					for(int c=0; c<columns.size(); c++){
						String columnName = columns.get(c);
						Column column = tab.findColumn(columnName);
						Object[] row = new Object[8];
						row[0] = Utils.getShort(DatabaseMetaData.bestRowSession);//SCOPE
						row[1] = columnName;			//COLUMN_NAME
						final int dataType = column.getDataType();
						row[2] = Utils.getInteger(dataType);//DATA_TYPE
						row[3] = SQLTokenizer.getKeyWord(dataType);//TYPE_NAME
						row[4] = Utils.getInteger(column.getPrecision());	//COLUMN_SIZE
														//BUFFER_LENGTH
						row[6] = Utils.getShort(column.getScale());		//DECIMAL_DIGITS
						row[7] = Utils.getShort(DatabaseMetaData.bestRowNotPseudo);//PSEUDO_COLUMN
						rows.add(row);
					}
				}
			}
		}
		Object[][] result = new Object[rows.size()][];
		rows.toArray(result);
		return result;		
	}

	
	Object[][] getPrimaryKeys(SSConnection con, String table) throws SQLException{
		List rows = new ArrayList();
		Strings tables = getTables(table);
		for(int t=0; t<tables.size(); t++){
    		String tableName = tables.get(t);
    		TableView tab = getTableView( con, tableName);
			if(!(tab instanceof Table)) continue;
			IndexDescriptions indexes = ((Table)tab).indexes;
			for(int i=0; i<indexes.size(); i++){
				IndexDescription index = indexes.get(i);
				if(index.isPrimary()){
					Strings columns = index.getColumns();
					for(int c=0; c<columns.size(); c++){
						Object[] row = new Object[6];
						row[0] = getName(); 			//TABLE_CAT
														//TABLE_SCHEM
						row[2] = tableName;				//TABLE_NAME
						row[3] = columns.get(c);		//COLUMN_NAME
						row[4] = Utils.getShort(c+1);	//KEY_SEQ
						row[5] = index.getName();		//PK_NAME
						rows.add(row);
					}
				}
			}
		}
		Object[][] result = new Object[rows.size()][];
		rows.toArray(result);
		return result;		
	}
	
	
	Object[][] getIndexInfo( SSConnection con, String table, boolean unique) throws SQLException {
		List rows = new ArrayList();
		Strings tables = getTables(table);
		Short type = Utils.getShort( DatabaseMetaData.tableIndexOther );
		for(int t=0; t<tables.size(); t++){
    		String tableName = tables.get(t);
    		TableView tab = getTableView( con, tableName);
			if(!(tab instanceof Table)) continue;
			IndexDescriptions indexes = ((Table)tab).indexes;
			for(int i=0; i<indexes.size(); i++){
				IndexDescription index = indexes.get(i);
				Strings columns = index.getColumns();
				for(int c=0; c<columns.size(); c++){
					Object[] row = new Object[13];
					row[0] = getName(); 			//TABLE_CAT
													//TABLE_SCHEM
					row[2] = tableName;				//TABLE_NAME
					row[3] = Boolean.valueOf(!index.isUnique());//NON_UNIQUE
													//INDEX_QUALIFIER
					row[5] = index.getName();		//INDEX_NAME
					row[6] = type;					//TYPE
					row[7] = Utils.getShort(c+1);	//ORDINAL_POSITION
					row[8] = columns.get(c);		//COLUMN_NAME
													//ASC_OR_DESC
													//CARDINALITY
													//PAGES
													//FILTER_CONDITION
					rows.add(row);
				}
			}
    	}
		Object[][] result = new Object[rows.size()][];
		rows.toArray(result);
		return result;
	}
}