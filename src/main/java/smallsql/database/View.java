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
 * View.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 31.05.2004
 */
package smallsql.database;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import smallsql.database.language.Language;

/**
 * @author Volker Berlin
 */
class View extends TableView{
	final String sql;
	final CommandSelect commandSelect;
	
	
	/**
	 * Constructor for loading an existing view. 
	 */
	View(SSConnection con, String name, FileChannel raFile, long offset) throws Exception{
		super( name, new Columns() );
		StorePage storePage = new StorePage( null, -1, raFile, offset);
		StoreImpl store = StoreImpl.createStore( null, storePage, SQLTokenizer.SELECT, offset);
		sql = store.readString();
		
		// read additional informations
		int type;
		while((type = store.readInt()) != 0){
			int offsetInPage = store.getCurrentOffsetInPage();
			int size = store.readInt();
			switch(type){
				//currently there are no additinal informations, see write()
			}
			store.setCurrentOffsetInPage(offsetInPage + size);
		}
		
		raFile.close();
		commandSelect = (CommandSelect)new SQLParser().parse(con, sql);
		createColumns(con);
	}
	
	
	/**
	 * Constructor for a new view. Is call on execute() of CREATE VIEW. This view is not init.
	 */
	View(Database database, SSConnection con, String name, String sql) throws Exception{
		super( name, new Columns() );
		this.sql  = sql;
		this.commandSelect = null;
		write(database, con);
	}

	
	/**
	 * Constructor for a UNION 
	 */
	View(SSConnection con, CommandSelect commandSelect) throws Exception{
		super("UNION", new Columns());
		this.sql = null;
		this.commandSelect = commandSelect;
		createColumns(con);
	}
	
	
	private void createColumns(SSConnection con) throws Exception{
		commandSelect.compile(con);
		Expressions exprs = commandSelect.columnExpressions;
		for(int c=0; c<exprs.size(); c++){
			Expression expr = exprs.get(c);
			if(expr instanceof ExpressionName){
				Column column = ((ExpressionName)expr).getColumn().copy();
				column.setName( expr.getAlias() );
				columns.add( column );
			}else{
				columns.add( new ColumnExpression(expr));
			}
		}
	}
	

	/**
	 * Drop the View. This method is static that the file does not need to load and also corrupt files can be dropped.
	 */ 
	static void drop(Database database, String name) throws Exception{
		File file = new File( Utils.createTableViewFileName( database, name ) );
		boolean ok = file.delete();
		if(!ok) throw SmallSQLException.create(Language.VIEW_CANTDROP, name);
	}
    

	private void write(Database database, SSConnection con) throws Exception{
	    FileChannel raFile = createFile( con, database );
		StorePage storePage = new StorePage( null, -1, raFile, 8);
		StoreImpl store = StoreImpl.createStore( null, storePage, SQLTokenizer.CREATE, 8);
		store.writeString(sql);		
		
		// write additional informations
		store.writeInt( 0 ); // no more additinal informations

		store.writeFinsh(null);
		raFile.close();
	}

	@Override
    void writeMagic(FileChannel raFile) throws Exception{
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putInt(MAGIC_VIEW);
        buffer.putInt(TABLE_VIEW_VERSION);
        buffer.position(0);
        raFile.write(buffer);
	}
	
}
