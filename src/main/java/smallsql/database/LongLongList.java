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
 * LongLongList.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.database;

/**
 * A list liek a vector that save 2 long values on on one position.
 * 
 * @author Volker Berlin
 */
final class LongLongList {
	private int size;
	private long[] data;

	LongLongList(){
		this(16);
	}
	LongLongList(int initialSize){
		data = new long[initialSize*2];
	}
	
	final int size(){
		return size;
	}

	final long get1(int idx){
		if (idx >= size)
			throw new IndexOutOfBoundsException("Index: "+idx+", Size: "+size);
		return data[idx << 1];
	}
	
	final long get2(int idx){
		if (idx >= size)
			throw new IndexOutOfBoundsException("Index: "+idx+", Size: "+size);
		return data[(idx << 1) +1];
	}
	
	final void add(long value1, long value2){
		int size2 = size << 1;
		if(size2 >= data.length ){
			resize(size2);
		}
		data[ size2   ] = value1;
		data[ size2 +1] = value2;
		size++;
	}

	final void clear(){
		size = 0;
	}
	
	private final void resize(int newSize){
		long[] dataNew = new long[newSize << 1];
		System.arraycopy(data, 0, dataNew, 0, size << 1);
		data = dataNew;		
	}
}
