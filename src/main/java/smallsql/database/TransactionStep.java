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
 * --------------------
 * TransactionStep.java
 * --------------------
 * Author: Volker Berlin
 * 
 * Created on 30.11.2008
 */
package smallsql.database;

import java.nio.channels.FileChannel;
import java.sql.SQLException;



/**
 * @author Volker Berlin
 */
abstract class TransactionStep{
    
    FileChannel raFile;
    
    TransactionStep(FileChannel raFile){
        this.raFile = raFile;
    }

    abstract long commit() throws SQLException;

    
    abstract void rollback() throws SQLException;
    
    void freeLock(){/* default nothing*/}

}
