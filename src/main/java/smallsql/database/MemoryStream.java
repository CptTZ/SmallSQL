/* =============================================================
 * SmallSQL : a free Java DBMS library for the Java(tm) platform
 * =============================================================
 *
 * (C) Copyright 2004-2011, by Volker Berlin.
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
 * MemoryStream.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.database;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author Volker Berlin
 *
 */
class MemoryStream {
	private byte[] puffer;
	private int offset;
	
	MemoryStream(){
		puffer = new byte[256];
	}
	
	void writeTo(FileChannel file) throws IOException{
	    ByteBuffer buffer = ByteBuffer.wrap( puffer, 0, offset );
		file.write(buffer);
	}
	
	void writeByte(int value){
		verifyFreePufferSize(1);
		puffer[ offset++ ] = (byte)(value);
	}
	
	void writeShort(int value){
		verifyFreePufferSize(2);
		puffer[ offset++ ] = (byte)(value >> 8);
		puffer[ offset++ ] = (byte)(value);
	}
	
	void writeInt(int value){
		verifyFreePufferSize(4);
		puffer[ offset++ ] = (byte)(value >> 24);
		puffer[ offset++ ] = (byte)(value >> 16);
		puffer[ offset++ ] = (byte)(value >> 8);
		puffer[ offset++ ] = (byte)(value);
	}
	
	void writeLong(long value){
		verifyFreePufferSize(8);
		puffer[ offset++ ] = (byte)(value >> 56);
		puffer[ offset++ ] = (byte)(value >> 48);
		puffer[ offset++ ] = (byte)(value >> 40);
		puffer[ offset++ ] = (byte)(value >> 32);
		puffer[ offset++ ] = (byte)(value >> 24);
		puffer[ offset++ ] = (byte)(value >> 16);
		puffer[ offset++ ] = (byte)(value >> 8);
		puffer[ offset++ ] = (byte)(value);
	}
	
	void writeChars(char[] value){
		verifyFreePufferSize(2*value.length);
		for(int i=0; i<value.length; i++){
			char c = value[i];
			puffer[ offset++ ] = (byte)(c >> 8);
			puffer[ offset++ ] = (byte)(c);
		}
	}
	
	void writeBytes(byte[] value, int off, int length){
		verifyFreePufferSize(length);
		System.arraycopy(value, off, puffer, offset, length);
		offset += length;
	}
	
	private void verifyFreePufferSize(int freeSize){
		int minSize = offset+freeSize;
		if(minSize < puffer.length){
			int newSize = puffer.length << 1;
			if(newSize < minSize) newSize = minSize;
			byte[] temp = new byte[newSize];
			System.arraycopy(puffer, 0, temp, 0, offset);
			puffer = temp;
		}
	}
    
    
    void skip(int count){
        offset += count;
    }
	
	
	int readByte(){
		return puffer[ offset++ ];
	}
	

	int readShort(){
		return ((puffer[ offset++ ] & 0xFF) << 8) | (puffer[ offset++ ] & 0xFF);
	}
	

	int readInt(){
		return ((puffer[ offset++ ] & 0xFF) << 24)
			 | ((puffer[ offset++ ] & 0xFF) << 16)
			 | ((puffer[ offset++ ] & 0xFF) << 8)
			 |  (puffer[ offset++ ] & 0xFF);
	}
	

	long readLong(){
		return (((long)(puffer[ offset++ ] & 0xFF)) << 56)
			 | (((long)(puffer[ offset++ ] & 0xFF)) << 48)
			 | (((long)(puffer[ offset++ ] & 0xFF)) << 40)
			 | (((long)(puffer[ offset++ ] & 0xFF)) << 32)
			 | ((puffer[ offset++ ] & 0xFF) << 24)
			 | ((puffer[ offset++ ] & 0xFF) << 16)
			 | ((puffer[ offset++ ] & 0xFF) << 8)
			 |  (puffer[ offset++ ] & 0xFF);
	}
	

	char[] readChars(int length){
		char[] chars = new char[length];
		for(int i=0; i<length; i++){
			chars[i] = (char)readShort();
		}
		return chars;
	}
	
	byte[] readBytes(int length){
		byte[] bytes = new byte[length];
		System.arraycopy(puffer, offset, bytes, 0, length);
		offset += length;
		return bytes;
	}
}
