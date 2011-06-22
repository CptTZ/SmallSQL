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
 * CommandDrop.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.database;

import java.io.*;
import smallsql.database.language.Language;

public class CommandDrop extends Command {

    CommandDrop( Logger log, String catalog, String name, int type ){
		super(log);
        this.type 		= type;
        this.catalog 	= catalog;
        this.name 		= name;
    }

    void executeImpl(SSConnection con, SSStatement st) throws Exception {
        switch(type){
            case SQLTokenizer.DATABASE:
                if(name.startsWith("file:"))
                    name = name.substring(5);
                File dir = new File( name );
                if(!dir.isDirectory() || 
                   !new File( dir, Utils.MASTER_FILENAME ).exists())
               			throw SmallSQLException.create(Language.DB_NONEXISTENT, name);
                File files[] = dir.listFiles();
                if(files != null)
	                for(int i=0; i<files.length; i++){
	                    files[i].delete();
	                }
                dir.delete();
                break;
            case SQLTokenizer.TABLE:
                Database.dropTable( con, catalog, name );
                break;
            case SQLTokenizer.VIEW:
				Database.dropView( con, catalog, name );
				break;
            case SQLTokenizer.INDEX:
            case SQLTokenizer.PROCEDURE:
                throw new java.lang.UnsupportedOperationException();
            default:
                throw new Error();
        }
    }
}