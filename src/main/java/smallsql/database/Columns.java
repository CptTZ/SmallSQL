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
 * Columns.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.database;

/**
 * A typed implementation of ArrayList for Column.
 * This list is used to describe the meta data of a ResultSet and to describe a Table.
 * 
 * @author Volker Berlin
 *
 */
final class Columns {
	private int size;
	private Column[] data;
	
	Columns(){
		data = new Column[16];
	}
	
	/*Columns(int initSize){
		data = new Column[initSize];
	}*/
	
	final int size(){
		return size;
	}

	final Column get(int idx){
		if (idx >= size)
			throw new IndexOutOfBoundsException("Column index: "+idx+", Size: "+size);
		return data[idx];
	}
	
    /**
     * Search for a Column with the given name. The search is not case sensitive.
     * 
     * @param name
     *            the name of the searching column.
     * @return The first found column or null.
     * @throws NullPointerException
     *             if the name is null.
     */
    final Column get(String name){
        for(int i = 0; i < size; i++){
            Column column = data[i];
            if(name.equalsIgnoreCase(column.getName())){
                return column;
            }
        }
        return null;
    }


    /**
     * Add a column to this list.
     * 
     * @param column
     *            the added column.
     * @throws NullPointerException
     *             if column is null
     */
    final void add(Column column){
        if(column == null){
            throw new NullPointerException("Column is null.");
        }
        if(size >= data.length){
            resize(size << 1);
        }
        data[size++] = column;
    }
	
	/*final void add(int idx, Column expr){
		if(size >= data.length ){
			resize(size << 1);
		}
		System.arraycopy( data, idx, data, idx+1, (size++)-idx);
		data[idx] = expr;
	}
	
	final void addAll(Columns cols){
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
	}*/

	/*final void set(int idx, Column expr){
		data[idx] = expr;
	}

	final int indexOf(Column expr) {
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
	}*/
	
    
    Columns copy(){
        Columns copy = new Columns();
        Column[] cols = copy.data = (Column[]) data.clone(); 
        for(int i=0; i<size; i++){
            cols[i] = cols[i].copy();
        }
        copy.size = size;
        return copy;
    }
    
    
	private final void resize(int newSize){
		Column[] dataNew = new Column[newSize];
		System.arraycopy(data, 0, dataNew, 0, size);
		data = dataNew;		
	}
}
