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
 * LongTreeList.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.database;

import java.sql.*;
import smallsql.database.language.Language;

/**
 * This class is used to save the row positions (RowID) list for a not unique index.
 *
 * The values for RowID are long (8 byte). The value differ around the row size. The
 * minimum row size is 30 byte. We calculate a medium row size of 100 bytes.
 *   
 * We used a tree to compress and sort the list. We save the long value in 4 levels
 * a 2 bytes. The first tree levels has a pointer to the next level. The end point 
 * of a level is the value 0. A value of 0 at first in a level means the value 0. 
 * The end point can only occur at second or later position and not on first position. 
 * 
 * 
 * @author Volker Berlin
 *
 */
final class LongTreeList {
	
	
	/*void list(){
		System.out.println("=========== size:"+size);
		LongTreeListEnum listEnum = new LongTreeListEnum();
		listEnum.reset();
		
		long value;
		do{
			value = getNext(listEnum);
			System.out.println(value);
		}while(value >0);
		do{
			value = getPrevious(listEnum);
			System.out.println(value);
		}while(value >0);
	}
	static public void main1(String[] argc) throws Exception{
		LongTreeList list = new LongTreeList();
		list.add( Long.MAX_VALUE/2 );
		list.list();
		list.add( Long.MAX_VALUE );
		list.list();
		list.remove( Long.MAX_VALUE/2 );
		list.list();
		list.add( 12345L );
		list.list();
		list.add( 123L );
		list.list();
		list.add( 12345678L );
		list.list();
		list.add( 12L );
		list.list();
		list.add( 1234L );
		list.list();
		list.add( 123456L );
		list.list();
		list.add( 1234567L );
		list.list();
		list.add( 123456789L );
		list.list();
		list.add( 123456790L );		
		list.list();
		list.add( 1L );
		list.list();
	}

	
	static public void main(String[] argc) throws Exception{
		java.util.Random random = new java.util.Random();
		LongTreeList treeList = new LongTreeList();
		java.util.ArrayList plainList = new java.util.ArrayList(); 
		LongTreeListEnum listEnum = new LongTreeListEnum();
		
		
		for(int i=1; i<1000; i++){
			long value;
			
			value = Math.abs(random.nextLong()) >> 6;
			//System.out.println(value+"  "+treeList.size);
			treeList.add(value);
			plainList.add(new Long(value));
		
			test(treeList, listEnum, plainList);
			
			if( i % 2 == 0){
				int idx = Math.abs(random.nextInt()) % plainList.size();
				value = ((Long)plainList.get( idx )).longValue();
				treeList.remove(value);
				plainList.remove(idx);
				
				test(treeList, listEnum, plainList);				
			}
		}
	}
	
	static void test(LongTreeList treeList, LongTreeListEnum listEnum, java.util.ArrayList plainList){ 
			listEnum.reset();
			int size = plainList.size();
			int count = 0;
			long value2, value = -1;
			do{
				value2 = value;
				value = treeList.getNext(listEnum);	
				if(value <0)break;
				if(value <= value2) throw new RuntimeException("wrong sort order:"+value+" and:"+value2);
				if(!plainList.contains(new Long(value))) throw new RuntimeException("wrong value:"+value);
				count++;
			}while(true);
			if(count != size) throw new RuntimeException("soll count:"+size+"   ist count:"+count);
			
			value = Long.MAX_VALUE;
			do{
				value2 = value;
				value = treeList.getPrevious(listEnum);
				if(value <0)break;
				if(value >= value2) throw new RuntimeException("wrong sort order:"+value+" and:"+value2);
				if(!plainList.contains(new Long(value))) throw new RuntimeException("wrong value:"+value);
				count--;
			}while(true);
			if(count != 0) throw new RuntimeException("Prevous count is wrong:"+count);
	}*/
	

	private byte[] data;
	private int size;
	private int offset;
	static final private int pointerSize = 3; //if change then also in resize()
	
	/**
	 * Create a empty LongTreeList.
	 *
	 */
	LongTreeList(){
		data = new byte[25];
	}
	
	/**
	 * Create a LongTreeList with a first value.
	 * @param value
	 */
	LongTreeList(long value) throws SQLException{
		this();
		add(value);
	}
	
	/**
	 * Restore a LongTreeList from a MemoryStream.
	 */
	LongTreeList(StoreImpl input){
		int readSize = input.readInt();
		data     = input.readBytes(readSize);
	}
	
	
	/**
	 * Save this list to a serial stream. This can be used to save it on a hard disk.
	 * @param output
	 */
	final void save(StoreImpl output){
		output.writeInt(size);
		output.writeBytes(data, 0, size);
	}
	

	/**
	 * Add a value to this list.
	 * @param value
	 * @throws SQLException
	 */
	final void add(long value) throws SQLException{
		offset = 0;
		if(size == 0){
			writeShort( (int)(value >> 48) );
			writePointer ( offset+pointerSize+2 );
			writeShort( 0 );
			writeShort( (int)(value >> 32) );
			writePointer ( offset+pointerSize+2 );
			writeShort( 0 );
			writeShort( (int)(value >> 16) );
			writePointer ( offset+pointerSize+2 );
			writeShort( 0 );
			writeShort( (int)(value) );
			writeShort( 0 );
			size = offset;
			return;
		}
		int shift = 48;
		boolean firstNode = (size > 2); // if this the first node in this tree level (0 can be the first node and are the end of the level)
		while(shift>=0){
			int octet = (int)(value >> shift) & 0xFFFF;
			while(true){
				int nextEntry = getUnsignedShort();
				if(nextEntry == octet){
					if(shift == 0) return; //value exist already, this case should not occur
					offset = getPointer();
					firstNode = true;
					break;
				}
				if((nextEntry == 0 && !firstNode) || nextEntry > octet){
					offset -= 2;
					while(true){
						if(shift != 0){
							offset = insertNode(octet);						
						}else{
							insertNodeLastLevel(octet);	
							return;
						}
						shift -= 16;
						octet = (int)(value >> shift) & 0xFFFF;
					}
				}
				firstNode = false;
				if(shift != 0) offset += pointerSize;
			}
			shift -= 16;
		}
	}
	
	
	/**
	 * Remove a value from this list.
	 * @param value
	 * @throws SQLException
	 */
	final void remove(long value) throws SQLException{
		if(size == 0) return;
		int offset1 = 0;
		int offset2 = 0;
		int offset3 = 0;
		offset = 0;
		int shift = 48;
		boolean firstNode = true; // if this the first node in this tree level (0 can be the first node and are the end of the level)
		boolean firstNode1 = true;
		boolean firstNode2 = true;
		boolean firstNode3 = true;
		while(shift>=0){
			int octet = (int)(value >> shift) & 0xFFFF;
			while(true){
				int nextEntry = getUnsignedShort();
				if(nextEntry == octet){
					if(shift == 0){
						//value find
						offset -= 2;
						removeNodeLastLevel();
						while(firstNode && getUnsignedShort() == 0){
							offset -= 2;
							removeNodeLastLevel(); // the end 0 of a node
							if(shift >= 3) 
								break;
							offset = offset1;
							offset1 = offset2;
							offset2 = offset3;
							firstNode = firstNode1;
							firstNode1 = firstNode2;
							firstNode2 = firstNode3;
							removeNode();
							shift++;
						}
						return;
					}
					offset3 = offset2;
					offset2 = offset1;
					offset1 = offset -2;
					offset = getPointer();
					firstNode3 = firstNode2;
					firstNode2 = firstNode1;
					firstNode1 = firstNode;
					firstNode = true;
					break;
				}
				if((nextEntry == 0 && !firstNode) || nextEntry > octet){
					//value is not in the list, this should not occur
					return;
				}
				firstNode = false;
				if(shift != 0) offset += pointerSize;
			}
			shift -= 16;
		}
	}
	

	/**
	 * Get the next long value from this list. 
     * If there are no more values then it return -1.
	 * @return
	 */
	final long getNext(LongTreeListEnum listEnum){
		int shift = (3-listEnum.stack) << 4;
		if(shift >= 64) return -1; //a previous call has return -1
		offset 		= listEnum.offsetStack[listEnum.stack];
		long result = listEnum.resultStack[listEnum.stack];
		boolean firstNode = (offset == 0); // true if it the first entry in a level
		while(true){
			int nextEntry = getUnsignedShort();
			if(nextEntry != 0 || firstNode){
				//there are more entries in this node
				result |= (((long)nextEntry) << shift);
				if(listEnum.stack>=3){
					listEnum.offsetStack[listEnum.stack] = offset;
					return result;
				}
				listEnum.offsetStack[listEnum.stack] = offset+pointerSize;
				offset = getPointer();
				shift -= 16;
				listEnum.stack++;
				listEnum.resultStack[listEnum.stack] = result;
				firstNode = true;
			}else{
				//no more entries in this node
				shift += 16;
				listEnum.stack--;
				if(listEnum.stack<0) return -1; // no more entries
				result = listEnum.resultStack[listEnum.stack];
				offset = listEnum.offsetStack[listEnum.stack];
				firstNode = false;
			}
		}
	}

	
	/**
	 * Get the next long value from this list.
     * If there are no more values then it return -1.
	 * @return
	 */
	final long getPrevious(LongTreeListEnum listEnum){
		int shift = (3-listEnum.stack) << 4;
		if(shift >= 64){ //a previous call of getNext() has return -1
			shift = 48;
			offset = 0;
			listEnum.stack = 0;
			listEnum.offsetStack[0] = 2 + pointerSize;
			loopToEndOfNode(listEnum);
		}else{
			setPreviousOffset(listEnum);
		}
		long result = listEnum.resultStack[listEnum.stack];
		while(true){
			int nextEntry = (offset < 0) ? -1 : getUnsignedShort();
			if(nextEntry >= 0){
				// there are more entries in this node
				result |= (((long)nextEntry) << shift);
				if(listEnum.stack>=3){
					listEnum.offsetStack[listEnum.stack] = offset;
					return result;
				}
				listEnum.offsetStack[listEnum.stack] = offset+pointerSize;
				offset = getPointer();
				shift -= 16;
				listEnum.stack++;
				listEnum.resultStack[listEnum.stack] = result;
				loopToEndOfNode(listEnum);
			}else{
				//no more entries in this node
				shift += 16;
				listEnum.stack--;
				if(listEnum.stack<0) return -1; // no more entries
				result = listEnum.resultStack[listEnum.stack];
				setPreviousOffset(listEnum);
			}
		}
	}
	
	
	/**
	 * Is used from getPrevious(). It set the offset of the previous entry.
	 * If there is no previous entry in this node then set it to -1.
	 * The problem is that "enum" point to the next position to optimize getNext().
	 * We need 2 steps forward to find the previous entry. It can occur that
	 * we are in another node. We need to verify it with the start point of the current node.
	 */
	final private void setPreviousOffset(LongTreeListEnum listEnum){
		int previousOffset = listEnum.offsetStack[listEnum.stack] - 2*(2 + (listEnum.stack>=3 ? 0 : pointerSize));
		if(listEnum.stack == 0){
			offset = previousOffset;
			return;
		}
		offset = listEnum.offsetStack[listEnum.stack-1] - pointerSize;
		int pointer = getPointer();
		if(pointer <= previousOffset){
			offset = previousOffset;
			return;
		}
		offset = -1;
	}
	
	
	/**
	 * Loop to the last entry in this node. Is used from getPrevious().
	 */
	final private void loopToEndOfNode(LongTreeListEnum listEnum){
		int nextEntry;
		int nextOffset1, nextOffset2;
		nextOffset1 = offset;
		offset += 2;
		if(listEnum.stack<3)
			offset += pointerSize;
		do{
			nextOffset2 = nextOffset1;
			nextOffset1 = offset;
			nextEntry = getUnsignedShort();
			if(listEnum.stack<3)
				offset += pointerSize;
		}while(nextEntry != 0);
		offset = nextOffset2;
	}
	
	
	

	/**
	 * Insert a octet entry on the current offset for one of the first 3 levels. 
	 * After it create a new node at the end (simple two 0). 
	 * Then set it the pointer in the new entry to the new node 
	 * @param octet a short value
	 * @return the offset of the new node.
	 */
	final private int insertNode(int octet) throws SQLException{
		int oldOffset = offset;
		
		if(data.length < size + 4 + pointerSize) resize();
		System.arraycopy(data, oldOffset, data, oldOffset + 2+pointerSize, size-oldOffset);
		size += 2+pointerSize;

		writeShort( octet );
		writePointer( size );

		//correct all offset that point behind the new node
		correctPointers( 0, oldOffset, 2+pointerSize, 0 );
		
		data[size++] = (byte)0;
		data[size++] = (byte)0;
		return size-2;
	}
	
		
	/**
	 * Insert the octet of the last level (4 level) on the current offset. 
	 * This level does not include a pointer to a next level.
	 * @param octet a short value
	 */
	final private void insertNodeLastLevel(int octet) throws SQLException{
		int oldOffset = offset;
				
		if(data.length < size + 2) resize();
		System.arraycopy(data, offset, data, offset + 2, size-offset);
		size += 2;
		writeShort( octet );
		
		//correct all offset before this new node that point behind the new node
		correctPointers( 0, oldOffset, 2, 0 );	
	}
	
	
	/**
	 * Remove a octet entry on the current offset for one of the first 3 levels. 
	 * Then set it the pointer in the new entry to the new node 
	 * @param octet a short value
	 */
	final private void removeNode() throws SQLException{
		int oldOffset = offset;
		
		//correct all offset that point behind the old node
		correctPointers( 0, oldOffset, -(2+pointerSize), 0 );

		size -= 2+pointerSize;
		System.arraycopy(data, oldOffset + 2+pointerSize, data, oldOffset, size-oldOffset);

		offset = oldOffset;
	}
	
	
	/**
	 * Remove a octet entry on the current offset for one of the first 3 levels. 
	 * Then set it the pointer in the new entry to the new node 
	 * @param octet a short value
	 */
	final private void removeNodeLastLevel() throws SQLException{
		int oldOffset = offset;
		
		//correct all offset that point behind the old node
		correctPointers( 0, oldOffset, -2, 0 );

		size -= 2;
		System.arraycopy(data, oldOffset + 2, data, oldOffset, size-oldOffset);

		offset = oldOffset;
	}
	
	
	/**
	 * Correct all pointers that point behind a new entry.
	 * @param startOffset the startoffset of the current node
	 * @param oldOffset the offset of the new entry, only pointer that point behind it need to correct.
	 * @param diff the differenz that need added to the pointers
	 * @param level the stack level. There are only 3 levels with pointers.
	 */
	final private void correctPointers(int startOffset, int oldOffset, int diff, int level){
		offset = startOffset;
		boolean firstNode = true;
		while(offset < size){
			if(offset == oldOffset){
				int absDiff = Math.abs(diff);
				if(absDiff == 2) return;
				offset += absDiff;
				firstNode = false;
				continue;
			}
			int value = getUnsignedShort();
			if(value != 0 || firstNode){
				int pointer = getPointer();
				if(pointer > oldOffset){
					offset  -= pointerSize;
					writePointer( pointer + diff );
					if(diff > 0) pointer += diff;
				}				
				if(level < 2){
					startOffset = offset;
					correctPointers( pointer, oldOffset, diff, level+1);
					offset = startOffset;
				}
				firstNode = false;
			}else{
				return;
			}
		}
	}
	
		
	/**
	 * Read a pointer to another node in de index.
	 */
	final private int getPointer(){
		int value = 0;
		for(int i=0; i<pointerSize; i++){
			value <<= 8;
			value += (data[offset++] & 0xFF);
		}
		return value;
	}
	
	
	/**
	 * Write a pointer to another node in the tree list. The size depends from the constant pointerSize.
	 */
	final private void writePointer(int value){
		for(int i=pointerSize-1; i>=0; i--){
			data[offset++] = (byte)(value >> (i*8));
		}
	}

	
	/**
	 * Read a short value from the index.
	 */
	final private int getUnsignedShort(){
		return ((data[ offset++ ] & 0xFF) << 8) | (data[ offset++ ] & 0xFF);
	}
	
	
	/**
	 * Save a short value in the index. The long values are saved in 4 short values-
	 * @param value
	 */
	final private void writeShort(int value){
		data[offset++] = (byte)(value >> 8);
		data[offset++] = (byte)(value);
	}
	
	
	/**
	 * Increment the buffer size for the list.
	 */
	private final void resize() throws SQLException{
		int newsize = data.length << 1;
		if(newsize > 0xFFFFFF){ //see pointerSize
			newsize = 0xFFFFFF;
			if(newsize == data.length) throw SmallSQLException.create(Language.INDEX_TOOMANY_EQUALS);
		}
		byte[] temp = new byte[newsize];
		System.arraycopy(data, 0, temp, 0, data.length);
		data = temp;
	}

	final int getSize() {
		return size;
	}
}
