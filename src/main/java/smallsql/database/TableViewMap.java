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
 * TableViewMap.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 22.06.2007
 */
package smallsql.database;

import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;



/**
 * @author Volker Berlin
 */
final class TableViewMap{
    private final HashMap map = new HashMap();
    
    
    /**
     * We save table and vies in a file with the same name. 
     * Some OS (like WIndows) accept different written names for the same file.
     * This method create an unique identifier for the same file.
     * As unique identifier can be use a java.io.File but this will produce different
     * results on different OS. This will it make difficult to transfer  
     * an application to another OS.
     * @param name the table or view name.
     * @return a unique object
     */
    private Object getUniqueKey(String name){
        return name.toUpperCase(Locale.US); //use the same locale for all
    }
    
    
    TableView get(String name){
        return (TableView)map.get(getUniqueKey(name));
    }


    void put(String name, TableView tableView){
        map.put(getUniqueKey(name), tableView);
    }


    TableView remove(String name){
        return (TableView)map.remove(getUniqueKey(name));
    }


    Collection values(){
        return map.values();
    }
}
