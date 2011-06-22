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
 * DataSource.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.database;



/**
 * This class implements the interface to access the data of a RowSource. 
 * It is a top level RowSource
 * It is a abstract class because interfaces are ever public.
 * 
 * Know Implementations are:
 * - TableResult
 * - GroupResult
 *
 *  * @author Volker Berlin
 *
 */
abstract class DataSource extends RowSource{


	abstract boolean isNull( int colIdx ) throws Exception;
	abstract boolean getBoolean( int colIdx ) throws Exception;

	abstract int getInt( int colIdx ) throws Exception;

	abstract long getLong( int colIdx ) throws Exception;

	abstract float getFloat( int colIdx ) throws Exception;

	abstract double getDouble( int colIdx ) throws Exception;

	abstract long getMoney( int colIdx ) throws Exception;

	abstract MutableNumeric getNumeric( int colIdx ) throws Exception;

	abstract Object getObject( int colIdx ) throws Exception;

	abstract String getString( int colIdx ) throws Exception;

	abstract byte[] getBytes( int colIdx ) throws Exception;

	abstract int getDataType( int colIdx );


	boolean init( SSConnection con ) throws Exception{return false;}
	String getAlias(){return null;}
	
	abstract TableView getTableView();
	
    
    /**
     * @inheritDoc
     */
	boolean isExpressionsFromThisRowSource(Expressions columns){
        for(int i=0; i<columns.size(); i++){
            ExpressionName expr = (ExpressionName)columns.get(i);
            if(this != expr.getDataSource()){
                return false;
            }
        }
        return true;
    }
	
}
