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
 * Expressions.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.database;

/**
 * @author Volker Berlin
 *
 */
final class Expressions {
	private int size;
	private Expression[] data;
	
	Expressions(){
		data = new Expression[16];
	}
	
	Expressions(int initSize){
		data = new Expression[initSize];
	}
	
	
	final int size(){
		return size;
	}
	

	final void setSize(int newSize){
		for(int i=newSize; i<size; i++) data[i] = null;
		size = newSize;
		if(size>data.length) resize(newSize);
	}
	

	final Expression get(int idx){
		if (idx >= size)
			throw new IndexOutOfBoundsException("Index: "+idx+", Size: "+size);
		return data[idx];
	}
	
	
	final void add(Expression expr){
		if(size >= data.length ){
			resize(size << 1);
		}
		data[size++] = expr;
	}

	final void add(int idx, Expression expr){
		if(size >= data.length ){
			resize(size << 1);
		}
		System.arraycopy( data, idx, data, idx+1, (size++)-idx);
		data[idx] = expr;
	}
	
	final void addAll(Expressions cols){
		int count = cols.size();
		if(size + count >= data.length ){
			resize(size + count);
		}
		System.arraycopy( cols.data, 0, data, size, count);
		size += count;
	}
	
	final void clear(){
		size = 0;
	}
	
	final void remove(int idx){
		System.arraycopy( data, idx+1, data, idx, (--size)-idx);
	}

	final void set(int idx, Expression expr){
		data[idx] = expr;
	}

	final int indexOf(Expression expr) {
		if (expr == null) {
			for (int i = 0; i < size; i++)
				if (data[i]==null)
					return i;
		} else {
			for (int i = 0; i < size; i++)
				if (expr.equals(data[i]))
					return i;
		}
		return -1;
	}
	
	final void toArray(Expression[] array){
		System.arraycopy( data, 0, array, 0, size);
	}
	
	final Expression[] toArray(){
		Expression[] array = new Expression[size];
		System.arraycopy( data, 0, array, 0, size);
		return array;
	}
	
	private final void resize(int newSize){
		Expression[] dataNew = new Expression[newSize];
		System.arraycopy(data, 0, dataNew, 0, size);
		data = dataNew;		
	}
}
