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
 * CommandSelect.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.database;

import java.sql.*;
import smallsql.database.language.Language;

class CommandSelect extends Command{

    private DataSources tables; // List of TableResult (Tables and Views)
	private Expression where;
    RowSource from;
    private Expressions groupBy;
    private Expression having;
    private Expressions orderBy;
    private boolean isAggregateFunction;
    private int maxRows = -1;
    /** is set if the keyword DISTINCT is used */
    private boolean isDistinct; 

    CommandSelect(Logger log){
		super(log);
    }
    
	CommandSelect(Logger log, Expressions columnExpressions){
		super(log, columnExpressions);
	}
    
    
    boolean compile(SSConnection con) throws Exception{
        boolean needCompile = false;
        if(tables != null){
            for(int i=0; i<tables.size(); i++){
				DataSource fromEntry = tables.get(i);
                needCompile |= fromEntry.init( con );
            }
        }

		if(from == null){
			from = new NoFromResult();
			tables = new DataSources();
			needCompile = true;
		}
        if(!needCompile) return false;

        for(int i=0; i<columnExpressions.size(); i++){
            Expression col = columnExpressions.get(i);
            if(col.getAlias() == null){
                // generate automate names for expressions
                col.setAlias("col" + (i+1));
            }

            if(col.getType() != Expression.NAME){
                compileLinkExpressionParams(col);
                continue;
            }

            ExpressionName expr = (ExpressionName)col;

            if("*".equals( expr.getName() )){
                String tableAlias = expr.getTableAlias();
                if(tableAlias != null){
                    // Syntax: tableAlias.*
                    int t=0;
                    for(; t<tables.size(); t++){
						DataSource fromEntry = tables.get(t);
                        if(tableAlias.equalsIgnoreCase( fromEntry.getAlias() )){
                            TableView table = fromEntry.getTableView();
                            columnExpressions.remove(i);
                            i = compileAdd_All_Table_Columns( fromEntry, table, i ) - 1;
                            break;
                        }
                    }
                    if(t==tables.size()) throw SmallSQLException.create(Language.COL_WRONG_PREFIX, new Object[] {tableAlias});
                }else{
                    // Syntax *
                    columnExpressions.remove(i);
                    for(int t=0; t<tables.size(); t++){
						DataSource fromEntry = tables.get(t);
                        TableView table = fromEntry.getTableView();
                        i = compileAdd_All_Table_Columns( fromEntry, table, i );
                    }
                    i--;
                }
            }else{
            	// not a * Syntax
                compileLinkExpressionName( expr );
            }

        }
        if(where != null) compileLinkExpression( where );
        if(having != null) compileLinkExpression( having );
        if(orderBy != null) {
            for(int i=0; i<orderBy.size(); i++){
            	compileLinkExpression( orderBy.get(i));
            }
        }
		if(groupBy != null){
			for(int i=0; i<groupBy.size(); i++){
				compileLinkExpression( groupBy.get(i) );
			}
		}

        if(from instanceof Join){
            compileJoin( (Join)from );
        }
        
        if(where != null){
        	from = new Where( from, where );
        }
        
		if(isGroupResult()) {
			from = new GroupResult( this, from, groupBy, having, orderBy);
			if(having != null){
                from = new Where( from, having );
            }
		}
		
		if(isDistinct){
			from = new Distinct( from, columnExpressions );
		}
		
		if(orderBy != null){
			from = new SortedResult( from, orderBy );
		}
		
		return true;
    }

    
    /**
     * If this ResultSet is use any type of grouping. This means that GroupResult need create and that
     * the ResultSet is not updatable. 
     */
    final boolean isGroupResult(){
    	return groupBy != null || having != null || isAggregateFunction;
    }
    
    
	/**
	 * Set the link between the Named Expression and the Table object
	 * in the condition.
	 * If there are cascade Joins then follow the tree with a recursion. 
	 */
    private void compileJoin( Join singleJoin ) throws Exception{
        if(singleJoin.condition != null) compileLinkExpressionParams( singleJoin.condition );
        if(singleJoin.left instanceof Join){
            compileJoin( (Join)singleJoin.left );
        }
        if(singleJoin.right instanceof Join){
            compileJoin( (Join)singleJoin.right );
        }
    }
    
    
    private void compileLinkExpression( Expression expr) throws Exception{
		if(expr.getType() == Expression.NAME)
			 compileLinkExpressionName( (ExpressionName)expr);
		else compileLinkExpressionParams( expr );
    }
    
    
    /**
     * Set the connection (link) of a named Expression to the table and the column index.
     * This means a column name in the SQL statement is link to it table source.
     */
    private void compileLinkExpressionName(ExpressionName expr) throws Exception{
        String tableAlias = expr.getTableAlias();
        if(tableAlias != null){
            int t = 0;
            for(; t < tables.size(); t++){
                DataSource fromEntry = tables.get(t);
                if(tableAlias.equalsIgnoreCase(fromEntry.getAlias())){
                    TableView table = fromEntry.getTableView();
                    int colIdx = table.findColumnIdx(expr.getName());
                    if(colIdx >= 0){
                        // Column was find and now we set the DataSouce, column index and TableView.
                        expr.setFrom(fromEntry, colIdx, table);
                        break;
                    }else
                        throw SmallSQLException.create(Language.COL_INVALID_NAME, new Object[]{expr.getName()});
                }
            }
            if(t == tables.size())
                throw SmallSQLException.create(Language.COL_WRONG_PREFIX, tableAlias);
        }else{
            // column name without table name
            boolean isSetFrom = false;
            for(int t = 0; t < tables.size(); t++){
                DataSource fromEntry = tables.get(t);
                TableView table = fromEntry.getTableView();
                int colIdx = table.findColumnIdx(expr.getName());
                if(colIdx >= 0){
                    if(isSetFrom){
                        // Column was already set. This means the column is ambiguous
                        throw SmallSQLException.create(Language.COL_AMBIGUOUS, expr.getName());
                    }
                    // Column was find and now we set the DataSouce, column index and TableView.
                    isSetFrom = true;
                    expr.setFrom(fromEntry, colIdx, table);
                }
            }
            if(!isSetFrom){
                throw SmallSQLException.create(Language.COL_INVALID_NAME, expr.getName());
            }
        }
        compileLinkExpressionParams(expr);
    }
    

    private void compileLinkExpressionParams(Expression expr) throws Exception{
        // check sub Expression (parameters)
        Expression[] expParams = expr.getParams();
		isAggregateFunction = isAggregateFunction || expr.getType() >= Expression.GROUP_BEGIN;
        if(expParams != null){
            for(int k=0; k<expParams.length; k++){
                Expression param = expParams[k];
				int paramType = param.getType();
				isAggregateFunction = isAggregateFunction || paramType >= Expression.GROUP_BEGIN;
                if(paramType == Expression.NAME)
                     compileLinkExpressionName( (ExpressionName)param );
                else compileLinkExpressionParams( param );
            }
        }
        expr.optimize();
    }
    

    private final int compileAdd_All_Table_Columns( DataSource fromEntry, TableView table, int position){
        for(int k=0; k<table.columns.size(); k++){
            ExpressionName expr = new ExpressionName( table.columns.get(k).getName() );
            expr.setFrom( fromEntry, k, table );
            columnExpressions.add( position++, expr );
        }
        return position;
    }
    

    /**
     * The main method to execute this Command and create a ResultSet.
     */
    void executeImpl(SSConnection con, SSStatement st) throws Exception{
        compile(con);
        if((st.rsType == ResultSet.TYPE_SCROLL_INSENSITIVE || st.rsType == ResultSet.TYPE_SCROLL_SENSITIVE) &&
        	!from.isScrollable()){
        	from = new Scrollable(from);
        }
        from.execute();
        rs =  new SSResultSet( st, this );
    }
    
    
    /**
     * Is used from ResultSet.beforeFirst().
     *
     */
    void beforeFirst() throws Exception{
		from.beforeFirst();
    }
    
    
	/**
	 * Is used from ResultSet.isBeforeFirst().
	 */
	boolean isBeforeFirst() throws SQLException{
		return from.isBeforeFirst();
	}
    

	/**
	 * Is used from ResultSet.isFirst().
	 */
	boolean isFirst() throws SQLException{
		return from.isFirst();
	}
    

	/**
     * Is used from ResultSet.first().
     */
    boolean first() throws Exception{
		return from.first();
    }


	/**
	 * Is used from ResultSet.previous().
	 */
	boolean previous() throws Exception{
		return from.previous();
	}


	/**
	 * move to the next row.
	 * @return true if the next row valid
	 * @throws Exception
	 */
    boolean next() throws Exception{
        if(maxRows >= 0 && from.getRow() >= maxRows){
        	from.afterLast();
        	return false;
        }
		return from.next();
    }


	/**
	 * Is used from ResultSet.last().
	 */
	final boolean last() throws Exception{
		if(maxRows >= 0){
            if(maxRows == 0){
                from.beforeFirst();
                return false;
            }
			return from.absolute(maxRows);
		}
		return from.last();
	}


	/**
	 * Is used from ResultSet.afterLast().
	 */
	final void afterLast() throws Exception{
		from.afterLast();
	}


	/**
	 * Is used from ResultSet.isLast().
	 */
	boolean isLast() throws Exception{
		return from.isLast();
	}
    

	/**
	 * Is used from ResultSet.isAfterLast().
	 */
	boolean isAfterLast() throws Exception{
		return from.isAfterLast();
	}
    

	/**
	 * Is used from ResultSet.absolute().
	 */
	final boolean absolute(int row) throws Exception{
		return from.absolute(row);
	}


	/**
	 * Is used from ResultSet.relative().
	 */
	final boolean relative(int rows) throws Exception{
		return from.relative(rows);
	}


	/**
	 * Is used from ResultSet.afterLast().
	 */
	final int getRow() throws Exception{
		int row = from.getRow();
		if(maxRows >= 0 && row > maxRows) return 0;
		return row;
	}


	final void updateRow(SSConnection con, Expression[] newRowSources) throws SQLException{
		int savepoint = con.getSavepoint();
		try{
			//loop through all tables of this ResultSet 
			for(int t=0; t<tables.size(); t++){
				TableViewResult result = TableViewResult.getTableViewResult( tables.get(t) );
				TableView table = result.getTableView();
				Columns tableColumns = table.columns;
				int count = tableColumns.size();
				
				// order the new Values after it position in the table
				Expression[] updateValues = new Expression[count];
				boolean isUpdateNeeded = false;
				for(int i=0; i<columnExpressions.size(); i++){
					Expression src = newRowSources[i];
					if(src != null && (!(src instanceof ExpressionValue) || !((ExpressionValue)src).isEmpty())){	
						Expression col = columnExpressions.get(i);
						if(!col.isDefinitelyWritable())
							throw SmallSQLException.create(Language.COL_READONLY, new Integer(i));
						ExpressionName exp = (ExpressionName)col;
						if(table == exp.getTable()){
							updateValues[exp.getColumnIndex()] = src;
							isUpdateNeeded = true;
							continue;
						}
					}
				}
				
				// save the new values if there are new value for this table
				if(isUpdateNeeded){
					result.updateRow(updateValues);
				}
			}
		}catch(Throwable e){
			con.rollback(savepoint);
			throw SmallSQLException.createFromException(e);
		}finally{
			if(con.getAutoCommit()) con.commit();
		}
	}
	
	final void insertRow(SSConnection con, Expression[] newRowSources) throws SQLException{
		if(tables.size() > 1)
			throw SmallSQLException.create(Language.JOIN_INSERT);
		if(tables.size() == 0)
			throw SmallSQLException.create(Language.INSERT_WO_FROM);
		
		int savepoint = con.getSavepoint();
		try{
			TableViewResult result = TableViewResult.getTableViewResult( tables.get(0) );
			TableView table = result.getTableView();
			Columns tabColumns = table.columns;
			int count = tabColumns.size();
					
			// order the new Values after it position in the table
			Expression[] updateValues = new Expression[count];
			if(newRowSources != null){
				for(int i=0; i<columnExpressions.size(); i++){
					Expression src = newRowSources[i];
					if(src != null && (!(src instanceof ExpressionValue) || !((ExpressionValue)src).isEmpty())){	
						Expression rsColumn = columnExpressions.get(i); // Column of the ResultSet
						if(!rsColumn.isDefinitelyWritable())
							throw SmallSQLException.create(Language.COL_READONLY, new Integer(i));
						ExpressionName exp = (ExpressionName)rsColumn;
						if(table == exp.getTable()){
							updateValues[exp.getColumnIndex()] = src;
							continue;
						}
					}
					updateValues[i] = null;
				}
			}
					
			// save the new values if there are new value for this table
			result.insertRow(updateValues);
		}catch(Throwable e){
			con.rollback(savepoint);
			throw SmallSQLException.createFromException(e);
		}finally{
			if(con.getAutoCommit()) con.commit();
		}
	}
	
	final void deleteRow(SSConnection con) throws SQLException{
		int savepoint = con.getSavepoint();
		try{
			if(tables.size() > 1)
				throw SmallSQLException.create(Language.JOIN_DELETE);
			if(tables.size() == 0)
				throw SmallSQLException.create(Language.DELETE_WO_FROM);
			TableViewResult.getTableViewResult( tables.get(0) ).deleteRow();
		}catch(Throwable e){
			con.rollback(savepoint);
			throw SmallSQLException.createFromException(e);
		}finally{
			if(con.getAutoCommit()) con.commit();
		}
	}
	
	
	/**
	 * The returning index start at 0.
	 */
	public int findColumn(String columnName) throws SQLException {
		Expressions columns = columnExpressions;
		// FIXME performance
		for(int i=0; i<columns.size(); i++){
			if(columnName.equalsIgnoreCase(columns.get(i).getAlias()))
				return i;
		}
		throw SmallSQLException.create(Language.COL_MISSING, columnName);
	}
	
	
	/**
	 * Set if the keyword DISTINCT occur in the SELECT expression. 
	 */
	final void setDistinct(boolean distinct){
		this.isDistinct = distinct;
	}
	
	
	/**
	 * Set the RowSource expression from the FROM clause. 
	 * The Simples case is only a Table (TableResult)
	 */
    final void setSource(RowSource join){
        this.from = join;
    }

	/**
	 * List of all Tables and Views. 
	 * This is needed to replace the table aliases in the columnExpressions with the real sources.
	 */
    final void setTables( DataSources from ){
        this.tables = from;
    }

	/**
	 * Is used from CommandSelect, CommandDelete and CommandUpdate
	 * @param where
	 */
	final void setWhere( Expression where ){
		this.where = where;
	}

	final void setGroup(Expressions group){
        this.groupBy = group;
    }

	final void setHaving(Expression having){
        this.having = having;
    }

	final void setOrder(Expressions order){
        this.orderBy = order;
    }
    

	final void setMaxRows(int max){
		maxRows = max;
	}
    
    
    final int getMaxRows(){
        return maxRows;
    }
}