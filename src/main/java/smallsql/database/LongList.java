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
 * LongList.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 05.07.2004
 */
package smallsql.database;

/**
 * A list of long values. The values are save in a array.
 * @author Volker Berlin
 */
class LongList {

	private int size;
	private long[] data;

	LongList(){
		this(16);
	}
	
	
	LongList(int initialSize){
		data = new long[initialSize];
	}
	
	
	final int size(){
		return size;
	}
	

	final long get(int idx){
		if (idx >= size)
			throw new IndexOutOfBoundsException("Index: "+idx+", Size: "+size);
		return data[idx];
	}
	
	
	final void add(long value){
		if(size >= data.length ){
			resize(size << 1);
		}
		data[ size++ ] = value;
	}
	

	final void clear(){
		size = 0;
	}
	
	
	private final void resize(int newSize){
		long[] dataNew = new long[newSize];
		System.arraycopy(data, 0, dataNew, 0, size);
		data = dataNew;		
	}

}
