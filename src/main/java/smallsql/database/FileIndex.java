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
 * FileIndex.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 24.09.2006
 */
package smallsql.database;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;


/**
 * @author Volker Berlin
 */
class FileIndex extends Index {

//static public void main(String args[]) throws Exception{
//    File file = File.createTempFile("test", "idx");
//    RandomAccessFile raFile = Utils.openRaFile(file, true );
//    FileIndex index = new FileIndex(false, raFile);
//    Expressions expressions = new Expressions();
//    ExpressionValue value = new ExpressionValue();
//    expressions.add(value);
//    value.set( "150", SQLTokenizer.VARCHAR);
//    index.addValues(1, expressions);
//    value.set( "15", SQLTokenizer.VARCHAR);
//    index.addValues(2, expressions);
//    print(index,expressions);
//    index.save();
//    index.close();
//    
//    System.out.println("Idx size:"+file.length());
//    raFile = Utils.openRaFile(file, true );
//    index = FileIndex.load(raFile);
//    print(index,expressions);
//}

static void print(Index index, Expressions expressions){
    IndexScrollStatus scroll = index.createScrollStatus(expressions);
    long l;
    while((l= scroll.getRowOffset(true)) >=0){
        System.out.println(l);
    }
    System.out.println("============================");
}

    
    private final FileChannel raFile;
    
    
    FileIndex( boolean unique, FileChannel raFile ) {
        this(new FileIndexNode( unique, (char)-1, raFile), raFile);
    }
    
    
    FileIndex( FileIndexNode root, FileChannel raFile ) {
        super(root);
        this.raFile = raFile;
    }
    
    
    static FileIndex load( FileChannel raFile ) throws Exception{
        ByteBuffer buffer = ByteBuffer.allocate(1);
        raFile.read(buffer);
        buffer.position(0);
        boolean unique = buffer.get() != 0;
        FileIndexNode root = FileIndexNode.loadRootNode( unique, raFile, raFile.position() );
        return new FileIndex( root, raFile );
    }
    
    
    void save() throws Exception{
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put(rootPage.getUnique() ? (byte)1 : (byte)0 );
        buffer.position(0);
        raFile.write( buffer );
        ((FileIndexNode)rootPage).save();
    }
    
    
    void close() throws IOException{
        raFile.close();
    }

}
