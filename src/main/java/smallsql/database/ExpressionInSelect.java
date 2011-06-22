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
 * ExpressionInSelect.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 07.08.2004
 */
package smallsql.database;

import smallsql.database.language.Language;

/**
 * @author Volker Berlin
 */
class ExpressionInSelect extends ExpressionArithmetic {

	final private CommandSelect cmdSel;
	final private Index index = new Index(true);
	final private SSConnection con;
	
	
	ExpressionInSelect(SSConnection con, Expression left, CommandSelect cmdSel, int operation) {
		super(left, (Expressions)null, operation);
		this.cmdSel = cmdSel;
		this.con = con;
	}

	
	private void loadInList() throws Exception{
		if(cmdSel.compile(con)){
			cmdSel.from.execute();
			if(cmdSel.columnExpressions.size() != 1)
				throw SmallSQLException.create(Language.SUBQUERY_COL_COUNT, new Integer(cmdSel.columnExpressions.size()));
			index.clear();
			while(cmdSel.next()){
				try{
					index.addValues(0, cmdSel.columnExpressions );
				}catch(Exception e){
					//double values can not add
				}
			}
		}
	}
	
	boolean isInList() throws Exception{
		loadInList();
		return index.findRows(getParams(), false, null) != null;
	}
}
