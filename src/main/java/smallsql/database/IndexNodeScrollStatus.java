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
 * IndexNodeScrollStatus.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 26.02.2005
 */
package smallsql.database;

/**
 * @author Volker Berlin
 */
final class IndexNodeScrollStatus {
	final boolean asc;
	final IndexNode[] nodes;
	/**
	 * A pointer of the last returns RowOffset. It can be the follow values. 
	 * -2; No Value, Before
	 * -1; The Value
	 * 0 to nodes.length-1; Any node from the array nodes.
	 * nodes.length; After the last valid node.
	 */
	int idx;
	final Object nodeValue;
	/**
	 * The current column of the ORDER BY clause. It is starting on 0.
	 */
	final int level;
	
	
	IndexNodeScrollStatus(IndexNode node, boolean asc, boolean scroll, int level){
		this.nodes = node.getChildNodes();
		nodeValue = node.getValue();
		this.asc = asc;
		this.idx = (asc ^ scroll) ? nodes.length : -2;
		this.level = level;
	}
	
	
	void afterLast(){
		idx = (asc) ? nodes.length : -2;			
	}
    
    
    /**
     * Check if the scrolling on this node is after the last entry. 
     * If this the scroll status of the root level then it is after
     * the last entry of the completely index.
     * If there are no entries then it is ever after the last entry. Can only occur on the root node.
     */
    /*boolean isAfterLast(){
        if(nodes.length == 0){ 
            return true;
        }
        return idx == ((asc) ? nodes.length : -2);
    }*/
}
