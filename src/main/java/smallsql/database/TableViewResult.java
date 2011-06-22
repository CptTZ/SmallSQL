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
 * TableViewResult.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 11.06.2004
 */
package smallsql.database;

import java.sql.SQLException;
import smallsql.database.language.Language;

/**
 * @author Volker Berlin
 */
abstract class TableViewResult extends DataSource {
	SSConnection con;
	
	private String alias;
	private long tableTimestamp;
	int lock = SQLTokenizer.SELECT;

	static TableViewResult createResult(TableView tableView){
		if(tableView instanceof Table)
			return new TableResult((Table)tableView);
		return new ViewResult( (View)tableView );
	}
	
	
	static TableViewResult getTableViewResult(RowSource from) throws SQLException{
		if(from instanceof Where){
			from = ((Where)from).getFrom();
		}
		if(from instanceof TableViewResult){
			return (TableViewResult)from;
		}
		throw SmallSQLException.create(Language.ROWSOURCE_READONLY);
	}
	
	void setAlias( String alias ){
		this.alias = alias;
	}
	

	String getAlias(){
		return (alias != null) ? alias : getTableView().name;
	}
	

	boolean hasAlias(){
		return alias != null;
	}
	
    
	/**
	 * Is used for compile() of different Commands
	 * 
	 * @param con
	 * @return true if now init; false if already init
	 * @throws Exception
	 */
	boolean init( SSConnection con ) throws Exception{
		TableView tableView = getTableView();
		if(tableTimestamp != tableView.getTimestamp()){
			this.con = con;
			tableTimestamp = tableView.getTimestamp();
			return true;
		}
		return false;
	}


	//abstract TableView getTableView();
	
	abstract void deleteRow() throws SQLException;

	/**
     * Replace the values of the current rows with the new values of the Expression array.
     * If an expression (not value) in the array null then the original value of the row is used.
	 * @param updateValues a list expressions that produce the new values
	 * @throws Exception if any error occur like conversions or io exceptions
	 */
	abstract void updateRow(Expression[] updateValues) throws Exception;
	
	abstract void insertRow(Expression[] updateValues) throws Exception;
	
	
	final boolean isScrollable(){
		return false;
	}

}
