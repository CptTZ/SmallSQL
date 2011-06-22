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
 * IndexNode.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.database;

import java.sql.*;
import smallsql.database.language.Language;

/**
 * @author Volker Berlin
 */
class IndexNode {
	final private boolean unique;
	final private char digit; // unsigned short
	
	static final private IndexNode[] EMPTY_NODES = new IndexNode[0];
	/**
	 * Can be a PageIndex to the next page or a byte[] with rest data of the key.
	 */
	private IndexNode[] nodes = EMPTY_NODES;
	
	/** 
	 * On this point of the tree there is no other value. There is only one value.
	 * That's the tree is cut here and the single value is saved. This is very large
	 * benefit if you have large strings and you can difference it with the 
	 * first characters. 
	 */
	private char[] remainderKey;

	/**
	 * Can save a Long, LongList with a rowOffset value or a IndexNode of the next root index.
	 */
	private Object value;

	/**
	 * The status array save the status of the digits. The follow table descript all valid combinations.<p><code>
	 * nodes     value         status
	 * ------------------------------------
	 * null      null          NO_ENTRY
	 * IndexNode null          NODE
	 * byte[]    Long/LongList REMAINDER_VALUE
	 * null      Long/LongList FINAL_VALUE
	 * IndexNode Long/LongList FINAL_VALUE + NODE
	 * null      IndexNode     ROOT
	 * byte[]    IndexNode     REMAINDER_VALUE + ROOT
	 * IndexNode IndexNode     NODE + ROOT
	 * 
	 * </code>
	 */
	
	/**
	 * Create a new Node in the Index. Do not call this constructor directly 
     * else use the factory method <code>createIndexNode</code>.
	 * @param unique descript if it is an unique index (primary key) or a multi value index is.
     * @see #createIndexNode
	 */
	protected IndexNode(boolean unique, char digit){
		this.unique = unique;
		this.digit  = digit;
	}
    
    
    /**
     * Create a new Node in the Index. This is a factory method 
     * that must be overridden from extended classes.
     * @param unique descript if it is an unique index (primary key) or a multi value index is.
     */
    protected IndexNode createIndexNode(boolean unique, char digit){
        return new IndexNode(unique, digit);
    }

	
	final char getDigit(){
		return digit;
	}
	
	
	final boolean getUnique(){
		return unique;
	}
	
	
	/**
	 * Returns the current status for the digit.
	 * @param digit The digit must be in the range 0 between 255. 
	 */
	final boolean isEmpty(){
		return nodes == EMPTY_NODES && value == null;
	}
	
	
	final void clear(){
		nodes = EMPTY_NODES;
		value = null;
		remainderKey = null;
	}
	
	
	final void clearValue(){
		value = null;
	}
	
	
	/**
	 * Returns the current value for the digit.
	 * @param digit The digit must be in the range 0 between 255. 
	 */
	final Object getValue(){
		return value;
	}
	
	
	final IndexNode[] getChildNodes(){
		return nodes;
	}
	
	
	/**
	 * Returns the IndexNode for the node position digit.
	 * @param digit The digit must be in the range 0 between 255. 
	 */
	final IndexNode getChildNode(char digit){
		int pos = findNodePos(digit);
		if(pos >=0) return nodes[pos];
		return null;
	}
	
	
	final char[] getRemainderValue(){
		return remainderKey;
	}
	
	
	/**
	 * Add a node in the middle of a key value.
	 * @param digit The digit must be in the range 0 between 255. 
	 */
	final IndexNode addNode(char digit) throws SQLException{
		if(remainderKey != null) moveRemainderValue();
		int pos = findNodePos( digit );
		if(pos == -1){
			IndexNode node = createIndexNode(unique, digit);
			saveNode( node );
			return node;
		}else{
			return nodes[pos];
		}
	}
	
	
	/**
	 * Remove a node.
	 * @param digit The digit must be in the range 0 between 255. 
	 */
	final void removeNode(char digit){
		int pos = findNodePos( digit );
		if(pos != -1){
			int length = nodes.length-1;
			IndexNode[] temp = new IndexNode[length];
			System.arraycopy(nodes, 0, temp, 0, pos);
			System.arraycopy(nodes, pos+1, temp, pos, length-pos);
			nodes = temp;
		}
	}
	
	
	/**
	 * Add a node on the end of a key value.
	 * @param digit The digit must be in the range 0 between 255. 
	 * @param rowOffset The value that is saved at the end of the tree.
	 */
	final void addNode(char digit, long rowOffset) throws SQLException{
		IndexNode node = addNode(digit);
		if(node.remainderKey != null) node.moveRemainderValue();
		node.saveValue(rowOffset);
	}
	
	
	/**
	 * Save the rowOffset on the digit position. This can be used for FINAL_VALUE or REMAINDER_VALUE.
	 * The caller need to verify that there already exist an equals value.
	 * This means that the digit and the remainder is equals.
	 * @param digit The digit must be in the range 0 between 255. 
	 * @param rowOffset The value that is saved in the tree.
	 */
	final void saveValue(long rowOffset) throws SQLException{
		if(unique){
			if(value != null) throw SmallSQLException.create(Language.KEY_DUPLICATE);
			value = new Long(rowOffset);
		}else{
			LongTreeList list = (LongTreeList)value;
			if(list == null){
				value = list = new LongTreeList();
			}
			list.add(rowOffset);
		}
	}
	

	/**
	 * Add a value on a tree node end without roll out the completly tree.
	 * This reduce the size of the tree if there are large enties with a high significance.
	 * for example: 
	 * If you have large strings which are different on the on the first 3 charchters. 
	 * Then you need only a tree size of 3. 
	 * @param digit 	 The digit must be in the range 0 between 255.
	 * @param rowOffset  The result value. This is the value that is saved in the tree.
	 * @param value 	 The key value.
	 * @param digitCount The count of digits from value that need to indexing in the tree. 
	 * 					 The range is from 1 to 3;  
	 */
	final void addRemainderKey(long rowOffset, long remainderValue, int charCount) throws SQLException{
		saveRemainderValue(remainderValue, charCount);
		value = (unique) ? (Object)new Long(rowOffset) : new LongTreeList(rowOffset);
	}
	
	
	final void addRemainderKey(long rowOffset, char[] remainderValue, int offset) throws SQLException{
		saveRemainderValue(remainderValue, offset);
		value = (unique) ? (Object)new Long(rowOffset) : new LongTreeList(rowOffset);
	}
	
	
	/**
	 * Add a new root index on the position of digit at the end of the tree. 
	 * This is needed for multi columns index at the end of the first (not last)
	 * column key value.
	 * @param digit The digit must be in the range 0 between 255. 
	 */
	final IndexNode addRoot(char digit) throws SQLException{
		IndexNode node = addNode(digit);
		if(node.remainderKey != null) node.moveRemainderValue();
		return node.addRoot();
	}
	
	
	final IndexNode addRootValue(char[] remainderValue, int offset) throws SQLException{
		saveRemainderValue(remainderValue, offset);
		return addRoot();
	}
	
	
	final IndexNode addRootValue( long remainderValue, int digitCount) throws SQLException{
		saveRemainderValue(remainderValue, digitCount);
		return addRoot();
	}
	
	
	/**
	 * Move a REMAINDER_VALUE node to the next node level.
	 * @param digit
	 * @throws SQLException
	 */
	private final void moveRemainderValue() throws SQLException{
		Object rowOffset = value;
		char[] puffer = remainderKey;
		value = null;
		remainderKey = null;
		IndexNode newNode = addNode(puffer[0]);
		if(puffer.length == 1){
			newNode.value  = rowOffset;
		}else{
			newNode.moveRemainderValueSub( rowOffset, puffer);
		}
	}
	
	
	private final void moveRemainderValueSub( Object rowOffset, char[] remainderValue){
		int length = remainderValue.length-1;
		this.remainderKey = new char[length];
		value = rowOffset;
		System.arraycopy( remainderValue, 1, this.remainderKey, 0, length);
	}
	

	private final void saveRemainderValue(char[] remainderValue, int offset){
		int length = remainderValue.length-offset;
		this.remainderKey = new char[length];
		System.arraycopy( remainderValue, offset, this.remainderKey, 0, length);
	}
	
	
	private final void saveRemainderValue( long remainderValue, int charCount){
		this.remainderKey = new char[charCount];
		for(int i=charCount-1, d=0; i>=0; i--){
			this.remainderKey[d++] = (char)(remainderValue >> (i<<4));
		}
	}
	
	/**
	 * Return the root IndexNode for the digit. If does not exists then it create one. 
	 * @param digit
	 */
	final IndexNode addRoot() throws SQLException{
		IndexNode root = (IndexNode)value;
		if(root == null){
			value = root = createIndexNode(unique, (char)-1);
		}
		return root;
	}
	
	
	private final void saveNode(IndexNode node){
		int length = nodes.length;
		IndexNode[] temp = new IndexNode[length+1];
		if(length == 0){
			temp[0] = node;
		}else{
			int pos = findNodeInsertPos( node.digit, 0, length);
			System.arraycopy(nodes, 0, temp, 0, pos);
			System.arraycopy(nodes, pos, temp, pos+1, length-pos);
			temp[pos] = node;
		}
		nodes = temp;
	}
	
	
	private final int findNodeInsertPos(char digit, int start, int end){
		if(start == end) return start;
		int mid = start + (end - start)/2;
		char nodeDigit = nodes[mid].digit;
		if(nodeDigit == digit) return mid;
		if(nodeDigit < digit){
			return findNodeInsertPos( digit, mid+1, end );
		}else{
			if(start == mid) return start;
			return findNodeInsertPos( digit, start, mid );
		}
	}
	

	private final int findNodePos(char digit){
		return findNodePos(digit, 0, nodes.length);
	}
	

	private final int findNodePos(char digit, int start, int end){
		if(start == nodes.length) return -1;
		int mid = start + (end - start)/2;
		char nodeDigit = nodes[mid].digit;
		if(nodeDigit == digit) return mid;
		if(nodeDigit < digit){
			return findNodePos( digit, mid+1, end );
		}else{
			if(start == mid) return -1;
			return findNodePos( digit, start, mid-1 );
		}
	}
    
	
	void save(StoreImpl output) throws SQLException{
		output.writeShort(digit);
		
		int length = remainderKey == null ? 0 : remainderKey.length;
		output.writeInt(length);
		if(length>0) output.writeChars(remainderKey);
		
		if(value == null){
			output.writeByte(0);
		}else
		if(value instanceof Long){
			output.writeByte(1);
			output.writeLong( ((Long)value).longValue() );
		}else
		if(value instanceof LongTreeList){
			output.writeByte(2);
			((LongTreeList)value).save(output);
		}else
		if(value instanceof IndexNode){
			output.writeByte(3);
			((IndexNode)value).saveRef(output);
		}
        
        output.writeShort(nodes.length);
        for(int i=0; i<nodes.length; i++){
            nodes[i].saveRef( output );
        }

	}
	
	
	
	void saveRef(StoreImpl output) throws SQLException{
		
	}
    
	
	IndexNode loadRef( long offset ) throws SQLException{
		throw new Error();
	}
    
	
	void load(StoreImpl input) throws SQLException{
		int length = input.readInt();
		remainderKey = (length>0) ? input.readChars(length) : null;
		
		int valueType = input.readByte();
		switch(valueType){
			case 0:
				value = null;
				break;
			case 1:
				value = new Long(input.readLong());
				break;
			case 2:
				value = new LongTreeList(input);
				break;
			case 3:
				value = loadRef( input.readLong());
				break;
			default: 
				throw SmallSQLException.create(Language.INDEX_CORRUPT, String.valueOf(valueType));
		}
        
        nodes = new IndexNode[input.readShort()];
        for(int i=0; i<nodes.length; i++){
            nodes[i] = loadRef( input.readLong() );
        }
	}
	

}
