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
 * StorePageMap.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 13.08.2004
 */
package smallsql.database;

/**
 * @author Volker Berlin
 */
class StorePageMap {



	/**
	 * The table, resized as necessary. Length MUST Always be a power of two.
	 */
	private Entry[] table;

	/**
	 * The number of key-value mappings contained in this identity hash map.
	 */
	private int size;
  
	/**
	 * The next size value at which to resize (capacity * load factor).
	 * @serial
	 */
	private int threshold;
  




	/**
	 * Constructs an empty <tt>HashMap</tt> with the default initial capacity
	 * (16) and the default load factor (0.75).
	 */
	StorePageMap() {
		threshold = 12;
		table = new Entry[17];
	}





 
	/**
	 * Returns the number of key-value mappings in this map.
	 *
	 * @return the number of key-value mappings in this map.
	 */
	final int size() {
		return size;
	}
  
	/**
	 * Returns <tt>true</tt> if this map contains no key-value mappings.
	 *
	 * @return <tt>true</tt> if this map contains no key-value mappings.
	 */
	final boolean isEmpty() {
		return size == 0;
	}

	/**
	 * Returns the first StorePage for the given key.
	 */
	final TableStorePage get(long key) {
		int i = (int)(key % table.length);
		Entry e = table[i]; 
		while (true) {
			if (e == null)
				return null;
			if (e.key == key) 
				return e.value;
			e = e.next;
		}
	}

	/**
	 * Returns <tt>true</tt> if this map contains a StorePage for the
	 * specified key.
	 *
	 */
	final boolean containsKey(long key) {
		return (get(key) != null);
	}

  
	/**
	 * Add the StorePage with the key. Multiple StorePage for the same key are valid.
	 * The cause are multiple changes in one transaction. With SavePoints a rollback to a older
	 * StorePage is valid.<p>
	 * The latest StorePage is placed at first pos.
	 */
	final TableStorePage add(long key, TableStorePage value) {
		int i = (int)(key % table.length);

		table[i] = new Entry(key, value, table[i]);
		if (size++ >= threshold) 
			resize(2 * table.length);
		return null;
	}


	/**
	 * Rehashes the contents of this map into a new array with a
	 * larger capacity.  This method is called automatically when the
	 * number of keys in this map reaches its threshold.
	 *
	 * If current capacity is MAXIMUM_CAPACITY, this method does not
	 * resize the map, but but sets threshold to Integer.MAX_VALUE.
	 * This has the effect of preventing future calls.
	 *
	 * @param newCapacity the new capacity, MUST be a power of two;
	 *        must be greater than current capacity unless current
	 *        capacity is MAXIMUM_CAPACITY (in which case value
	 *        is irrelevant).
	 */
	final private void resize(int newCapacity) {

		Entry[] newTable = new Entry[newCapacity];
		transfer(newTable);
		table = newTable;
		threshold = (int)(newCapacity * 0.75f);
	}

	/** 
	 * Transfer all entries from current table to newTable.
	 */
	final private void transfer(Entry[] newTable) {
		Entry[] src = table;
		int newCapacity = newTable.length;
		for (int j = 0; j < src.length; j++) {
			Entry e = src[j];
			if (e != null) {
				src[j] = null;
				do {
					Entry next = e.next;
					e.next = null;
					int i = (int)(e.key % newCapacity);
					//The order for StorePages with the same key must not change 
					//that we need to find the end of the link list. This is different to a typical HashTable
					if(newTable[i] == null){
						newTable[i] = e;
					}else{
						Entry entry = newTable[i];
						while(entry.next != null) entry = entry.next;
						entry.next = e;
					}
					e = next;
				} while (e != null);
			}
		}
	}

  
	/**
	 * Removes the mapping for this key from this map if present.
	 *
	 * @param  key key whose mapping is to be removed from the map.
	 * @return previous value associated with specified key, or <tt>null</tt>
	 *	       if there was no mapping for key.  A <tt>null</tt> return can
	 *	       also indicate that the map previously associated <tt>null</tt>
	 *	       with the specified key.
	 */
	final TableStorePage remove(long key) {
		int i = (int)(key % table.length);
		Entry prev = table[i];
		Entry e = prev;

		while (e != null) {
			Entry next = e.next;
			if (e.key == key) {
				size--;
				if (prev == e) 
					table[i] = next;
				else
					prev.next = next;
				return e.value;
			}
			prev = e;
			e = next;
		}
		return null;
	}


	/**
	 * Removes all mappings from this map.
	 */
	final void clear() {
		Entry tab[] = table;
		for (int i = 0; i < tab.length; i++) 
			tab[i] = null;
		size = 0;
	}

	/**
	 * Returns <tt>true</tt> if this map maps one or more keys to the
	 * specified value.
	 *
	 * @param value value whose presence in this map is to be tested.
	 * @return <tt>true</tt> if this map maps one or more keys to the
	 *         specified value.
	 */
	final boolean containsValue(TableStorePage value) {
		Entry tab[] = table;
			for (int i = 0; i < tab.length ; i++)
				for (Entry e = tab[i] ; e != null ; e = e.next)
					if (value.equals(e.value))
						return true;
		return false;
	}



	static class Entry{
		final long key;
		final TableStorePage value;
		Entry next;

		/**
		 * Create new entry.
		 */
		Entry(long k, TableStorePage v, Entry n) { 
			value = v; 
			next = n;
			key = k;
		}

    
	}


}
