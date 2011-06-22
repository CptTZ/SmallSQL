/* =============================================================
 * SmallSQL : a free Java DBMS library for the Java(tm) platform
 * =============================================================
 *
 * (C) Copyright 2004-2006, by Volker Berlin.
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
 * TableStorePageInsert.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 16.08.2004
 */
package smallsql.database;

import java.sql.*;

/**
 * This is a TableStorePage for insert operations. The advantages is a link.
 * Because a new StorePage has no filepos before it was commit it is difficult to refind the StorePage
 * after if was commit  
 * With this link the StorePage can be refind if it was commit and save or not.
 * @author Volker Berlin
 */
class TableStorePageInsert extends TableStorePage {

	final private StorePageLink link = new StorePageLink();

	TableStorePageInsert(SSConnection con, Table table, int lockType){
		super( con, table, lockType, -1);
		link.page = this;
		link.filePos = fileOffset;
	}
	

	/**
	 * Call supper.commit() and update the link.
	 */
	final long commit() throws SQLException{
		long result = super.commit();
		link.filePos = fileOffset;
		link.page = null;
		return result;
	}
	
	
	/**
	 * Return the link to this StorePage
	 */
	final StorePageLink getLink(){
		return link;
	}
}
