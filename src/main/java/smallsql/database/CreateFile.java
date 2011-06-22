/* =============================================================
 * SmallSQL : a free Java DBMS library for the Java(tm) platform
 * =============================================================
 *
 * (C) Copyright 2008-2011, by Volker Berlin.
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
 * CreateFile.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.database;

import java.io.File;
import java.nio.channels.FileChannel;
import java.sql.SQLException;

import smallsql.database.language.Language;

public class CreateFile extends TransactionStep{

    private final File file;
    private final SSConnection con;
    private final Database database;


    CreateFile(File file, FileChannel raFile,SSConnection con, Database database){
        super(raFile);
        this.file = file;
        this.con = con;
        this.database = database;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    long commit(){
        raFile = null;
        return -1;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    void rollback() throws SQLException{
        FileChannel currentRaFile = raFile;
        if(raFile == null){
            return;
        }
        raFile = null;
        try{
            currentRaFile.close();
        }catch(Throwable ex){
            //ignore it
        }
        con.rollbackFile(currentRaFile);
        if(!file.delete()){
            file.deleteOnExit();
            throw SmallSQLException.create(Language.FILE_CANT_DELETE, file.getPath());
        }
        
        String name = file.getName();
        name = name.substring(0, name.lastIndexOf('.'));
        database.removeTableView(name);
    }

}
