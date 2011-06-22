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
 * IndexScrollStatus.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 18.04.2005
 */
package smallsql.database;

/**
 * This class save the status of the scrolling through a Index. This is like a Cursor.
 */
class IndexScrollStatus {
	private final IndexNode rootPage;
	private final Expressions expressions; // is used for the description of ASC and DESC

	private final java.util.Stack nodeStack = new java.util.Stack(); //TODO performance Stack durch nicht synchronisierte Klasse ersetzten
	/** Used for getRowOffset() as context cash between 2 calls */
	private LongTreeList longList;
	private LongTreeListEnum longListEnum = new LongTreeListEnum();


	IndexScrollStatus(IndexNode rootPage, Expressions expressions){	
		this.rootPage	= rootPage;
		this.expressions= expressions;
		reset();
	}
	
	
	/**
	 * Reset this index to start a new scan of it with nextRowoffset()
	 */
	final void reset(){
		nodeStack.clear();
		boolean asc = (expressions.get(0).getAlias() != SQLTokenizer.DESC_STR);
		nodeStack.push( new IndexNodeScrollStatus(rootPage, asc, true, 0) );
	}

	
	/**
	 * Return the next rowOffset of this index. You need to call reset() before the first use. 
	 * @param next if true the next rowOffset else the previous rowOffset
	 */
	final long getRowOffset( boolean scroll){
		if(longList != null){
			long rowOffset = scroll ? 
								longList.getNext(longListEnum) : 
								longList.getPrevious(longListEnum);
			if(rowOffset < 0){
				// No more entries on this node
				longList = null;
			}else{
				return rowOffset;
			}
		}
		while(true){
			IndexNodeScrollStatus status = (IndexNodeScrollStatus)nodeStack.peek();
			int level = status.level;
			if(!status.asc ^ scroll){
				//ASC order
				int idx = ++status.idx;
				if(idx == -1){
					if(status.nodeValue != null){
						if(status.nodeValue instanceof IndexNode){
							level++;
							nodeStack.push(
								new IndexNodeScrollStatus( 	(IndexNode)status.nodeValue, 
														(expressions.get(level).getAlias() != SQLTokenizer.DESC_STR), 
														scroll, level));
							continue;
						}else
							return getReturnValue(status.nodeValue);
					}
					//There is no RowOffset in this node
					idx = ++status.idx;
				}
				if(idx >= status.nodes.length){
					//No more nodes in this level
					if(nodeStack.size() > 1){
						nodeStack.pop();
						continue;
					}else{
						//No more RowOffsets in this Index
                        status.idx = status.nodes.length; //to prevent problems with scroll back after multiple calls after the end.
						return -1;
					}
				}
				IndexNode node = status.nodes[idx];
				nodeStack.push( new IndexNodeScrollStatus(node, status.asc, scroll, level) );
			}else{
				//DESC order
				int idx = --status.idx;
				if(idx == -1){
					if(status.nodeValue != null){
						if(status.nodeValue instanceof IndexNode){
							level++;
							nodeStack.push(
								new IndexNodeScrollStatus( 	(IndexNode)status.nodeValue, 
														(expressions.get(level).getAlias() != SQLTokenizer.DESC_STR), 
														scroll, level));
							continue;
						}else
							return getReturnValue(status.nodeValue);
					}
					//There is no RowOffset in this node
				}
				if(idx < 0){
					//No more nodes in this level
					if(nodeStack.size() > 1){
						nodeStack.pop();
						continue;
					}else{
						//No more RowOffsets in this Index
						return -1;
					}
				}
				IndexNode node = status.nodes[idx];
				nodeStack.push( new IndexNodeScrollStatus(node, status.asc, scroll, level) );
			}
		}
	}
		

	/**
	 * Move the index after the last position. The next call nextRowOffset() returns a -1
	 *
	 */
	final void afterLast(){
		longList = null;
		nodeStack.setSize(1);
		((IndexNodeScrollStatus)nodeStack.peek()).afterLast();
	}
	
    
    /**
     * Check if the index is after the last position.
     */
    /*final boolean isAfterLast(){
        if(longList != null || nodeStack.size() != 1){
            return false;
        }
        return ((IndexNodeScrollStatus)nodeStack.peek()).isAfterLast();
    }*/
    
	
	private final long getReturnValue( Object value){
		if(rootPage.getUnique()){
			return ((Long)value).longValue();
		}else{
			longList = (LongTreeList)value;
			longListEnum.reset();
			return longList.getNext(longListEnum); // there be should one value as minimum
		}
		
	}
	
	
		

}
