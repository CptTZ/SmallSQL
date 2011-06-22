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
 * FileIndexNode.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.database;

import java.io.*;
import java.nio.channels.FileChannel;
import java.sql.SQLException;
/**
 * @author Volker Berlin
 *
 */
public class FileIndexNode extends IndexNode {

	private final FileChannel file;
	private long fileOffset;
	
    
	/**
	 * Create a new Node in the Index.
	 * @param unique describe if it is an unique index (primary key) or a multi value index is.
	 */
	FileIndexNode(boolean unique, char digit, FileChannel file){
		super(unique, digit);
		this.file = file;
        fileOffset = -1;
	}
    
	
    @Override
    protected IndexNode createIndexNode(boolean unique, char digit){
        return new FileIndexNode(unique, digit, file);
    }
    
    
	void save() throws SQLException{
        StorePage storePage = new StorePage( null, -1, file, fileOffset);
        StoreImpl store = StoreImpl.createStore( null, storePage, SQLTokenizer.INSERT, fileOffset);
		save(store);
        fileOffset = store.writeFinsh(null);
	}
	
	@Override
    void saveRef(StoreImpl output) throws SQLException{
        if(fileOffset < 0){
            save();
        }
		output.writeLong(fileOffset);
	}
	
    @Override
    IndexNode loadRef( long offset ) throws SQLException{
        StorePage storePage = new StorePage( null, -1, file, offset);
        StoreImpl store = StoreImpl.createStore( null, storePage, SQLTokenizer.INSERT, fileOffset);
        MemoryStream input = new MemoryStream();
		FileIndexNode node = new FileIndexNode( getUnique(), (char)input.readShort(), file );
		node.fileOffset = offset;
        node.load( store );
		return node;	
	}
    
    static FileIndexNode loadRootNode(boolean unique, FileChannel file, long offset) throws Exception{
        StorePage storePage = new StorePage( null, -1, file, offset);
        StoreImpl store = StoreImpl.createStore( null, storePage, SQLTokenizer.SELECT, offset);
        FileIndexNode node = new FileIndexNode( unique, (char)store.readShort(), file );
        node.fileOffset = offset;
        node.load( store );
        return node;    
    }
}
