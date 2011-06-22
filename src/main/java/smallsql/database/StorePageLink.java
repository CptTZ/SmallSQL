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
 * StorePageLink.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 09.08.2004
 */
package smallsql.database;

/**
 * This class is a reference to a StorePage. The link can point to a uncommited StorePage in Memory
 * or it can link to a filepos.
 * 
 * @author Volker Berlin
 */
class StorePageLink {
	long filePos;
	TableStorePage page;
	
	StoreImpl getStore(Table table, SSConnection con, int lock) throws Exception{
		TableStorePage page = this.page;
		if(page == null)
			return table.getStore( con, filePos, lock );
		while(page.nextLock != null) page = page.nextLock;
		return table.getStore( page, lock);
	}
}
