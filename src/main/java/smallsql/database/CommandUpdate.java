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
 * CommandUpdate.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.database;


/**
 * @author Volker Berlin
 *
 */
class CommandUpdate extends CommandSelect {

	private Expressions sources = new Expressions();
	
	private Expression[] newRowSources;
	
	CommandUpdate( Logger log ){
		super(log);
	}
	
	/**
	 * Set a value pair of the update command. For example:
	 * "UPDATE table1 SET col1 = 234"
	 * "col1" is the dest
	 * "234" is the source
	 * @param dest
	 * @param source must be a ExpressionName
	 */
	void addSetting(Expression dest, Expression source){
		//destinations.add(dest);
		columnExpressions.add(dest);
		sources.add(source);
	}
	
	
	/*boolean compile(SSConnection con) throws Exception{
		if(super.compile(con)){
			TableView table = TableViewResult.getTableViewResult(join).getTableView();
			newRowSources = new Expression[table.columns.size()];
			for(int i=0; i<destinations.size(); i++){
				String name = destinations.get(i).getName();
				int colIdx = table.findColumn( name );
				if(colIdx>=0){
					// Column found 
					newRowSources[colIdx] = sources.get(i);
					break;
				}else
					throw SmallSQLException.createSQLException(Language.Invalid column name '" + name + "'.");
			}
			return true;
		}
		return false;		
	}*/
	
	
	void executeImpl(SSConnection con, SSStatement st) throws Exception {
		int count = columnExpressions.size();
		columnExpressions.addAll(sources);
		compile(con);
		columnExpressions.setSize(count);
		newRowSources = sources.toArray();
		updateCount = 0;
		from.execute();
		
		// Change the lock on all reading table to write lock
		// this is needed for the case that a writing value depends on reading value 
		for(int i=0; i<columnExpressions.size(); i++){
		    ExpressionName expr = (ExpressionName)columnExpressions.get(i);
		    DataSource ds = expr.getDataSource();
		    TableResult tableResult = (TableResult)ds;
		    tableResult.lock = SQLTokenizer.UPDATE;
		}
		
		while(true){
			// the reading and writing of a row must be atomic
            synchronized(con.getMonitor()){
                if(!next()){
                    return;
                }
                updateRow(con, newRowSources);
            }
			updateCount++;
		}
	}
}
