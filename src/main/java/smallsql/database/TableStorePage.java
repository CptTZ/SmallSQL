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
 * TableStorePage.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.database;

import java.sql.*;

public class TableStorePage extends StorePage{
    final Table table;
    
    // ======= variables needed for locking ========
	int lockType;
	SSConnection con;
	TableStorePage nextLock;


	TableStorePage(SSConnection con, Table table, int lockType, long fileOffset){
		super( null, 0, table.raFile, fileOffset );
		this.con 	= con;
		this.table = table;
		this.lockType 	= lockType;
	}
	   
    
    byte[] getData(){
    	return page;
    }


    /**
     * Returns the final position of the page back.
     */
    long commit() throws SQLException{
		if(nextLock != null){
			// save only the last version of this page
			fileOffset = nextLock.commit();
			nextLock = null;
			rollback();
			return fileOffset;
		}
    	if(lockType == TableView.LOCK_READ)
    		return fileOffset;
    	return super.commit();
    }

    
    final void freeLock(){
    	table.freeLock(this);
    }
}