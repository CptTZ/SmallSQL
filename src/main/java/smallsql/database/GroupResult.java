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
 * GroupResult.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.database;

import java.sql.*;
import smallsql.database.language.Language;

/**
 * @author Volker Berlin
 *
 */
class GroupResult extends MemoryResult{

	private Expression currentGroup; //Validate if the current row of cmd is part of the current group
	private RowSource from;
	private Expressions groupBy; // the list of Expressions in the GROUP BY clause
    private Expressions expressions = new Expressions(); // List of Expression
	private Expressions internalExpressions = new Expressions(); // a list of Aggregate Function and ColNames from SELECT, GROUP BY and HAVING
	
	/**
	 * Constructor for Grouping a Result from a CommandSelect
	 */
	GroupResult(CommandSelect cmd, RowSource from, Expressions groupBy, Expression having, Expressions orderBy) throws SQLException{
		this.from = from;
		this.groupBy = groupBy;
		
		if(groupBy != null){
			for(int i=0; i<groupBy.size(); i++){
				Expression left = groupBy.get(i);
				int idx = addInternalExpressionFromGroupBy( left );
				ExpressionName right = new ExpressionName(null);
				right.setFrom(this, idx, new ColumnExpression(left));
				Expression expr = new ExpressionArithmetic( left, right, ExpressionArithmetic.EQUALS_NULL);
				currentGroup = (currentGroup == null) ? 
								expr :
								new ExpressionArithmetic( currentGroup, expr, ExpressionArithmetic.AND );
			}
		}
		expressions = internalExpressions;
        for(int c=0; c<expressions.size(); c++){
            addColumn(new ColumnExpression(expressions.get(c)));
        }

		patchExpressions( cmd.columnExpressions );
		if(having != null) having = patchExpression( having );
		patchExpressions( orderBy );
	}
	
	/**
	 * Add a expression to the internal expression list if not exist in this list.
	 * It will be added named columns in the GROUP BY clause.
	 * @param expr The expression to added.
	 * @return the position in the internal list
	 */
	final private int addInternalExpressionFromGroupBy(Expression expr) throws SQLException{
		int type = expr.getType();
		if(type >= Expression.GROUP_BEGIN){
				throw SmallSQLException.create(Language.GROUP_AGGR_INVALID, expr);
		}else{
			int idx = internalExpressions.indexOf(expr);
			if(idx >= 0) return idx;
			internalExpressions.add(expr);
			return internalExpressions.size()-1;
		}
	}
	
	
	/**
	 * Add a expression to the internal expression list if not exist in this list.
	 * It will be added aggregate functions from the SELECT, HAVING and ORDER BY clause.
	 * @param expr The expression to added.
	 * @return the position in the internal list
	 */
	final private int addInternalExpressionFromSelect(Expression expr) throws SQLException{
		int type = expr.getType();
		if(type == Expression.NAME){
			int idx = internalExpressions.indexOf(expr);
			if(idx >= 0) return idx;
			throw SmallSQLException.create(Language.GROUP_AGGR_NOTPART, expr);
		}else
		if(type >= Expression.GROUP_BEGIN){
			int idx = internalExpressions.indexOf(expr);
			if(idx >= 0) return idx;
			internalExpressions.add(expr);
			return internalExpressions.size()-1;
		}else{
			//if a function or arithmetic expression is already in the group by the it is OK
			int idx = internalExpressions.indexOf(expr);
			if(idx >= 0) return idx;
			Expression[] params = expr.getParams();
			if(params != null){
				for(int p=0; p<params.length; p++){
					addInternalExpressionFromSelect( params[p]);
				}
			}
			return -1;
		}
	}
	
	
	/**
	 * Patch all external ExpressionName in the list (SELECT clause)
	 * that it link to the the internal RowSource.
	 */
	final private void patchExpressions(Expressions exprs) throws SQLException{
		if(exprs == null) return;
		for(int i=0; i<exprs.size(); i++){
			exprs.set(i, patchExpression(exprs.get(i)));
		}	
	}
	
	
	final private void patchExpressions(Expression expression) throws SQLException{
		Expression[] params = expression.getParams();
		if(params == null) return;
		for(int i=0; i<params.length; i++){
			expression.setParamAt( patchExpression(params[i]), i);
		}
	}
	
	
	/**
	 * Patch a single Expression. The caller need to replace the original Object
	 * if the return value return another object.
	 * @param expr the Expression to patch
	 * @return on simple columns and Aggregatfunction the original Expression is return as patch.
	 */
	final private Expression patchExpression(Expression expr) throws SQLException{
		//find the index in the internalExpression list
		int idx = addInternalExpressionFromSelect( expr );
		if(idx>=0){
            Expression origExpression = expr;
			ExpressionName exprName;
			if(expr instanceof ExpressionName){
				exprName = (ExpressionName)expr;
			}else{
				// this can only occur if in the GROUP BY clause are a function or arithmetic expression
				// and a equals expression is used in SELECT, GROUP BY or HAVING
				expr = exprName = new ExpressionName(expr.getAlias());
			}
			// patch the expression and set a new DataSource
			Column column = exprName.getColumn();
			if(column == null){
				column = new Column();
                exprName.setFrom(this, idx, column);
				switch(exprName.getType()){
					case Expression.MAX:
					case Expression.MIN:
					case Expression.FIRST:
					case Expression.LAST:
					case Expression.SUM:
						Expression baseExpression = exprName.getParams()[0];
						column.setPrecision(baseExpression.getPrecision());
						column.setScale(baseExpression.getScale());
						break;
                    default:
                        column.setPrecision(origExpression.getPrecision());
                        column.setScale(origExpression.getScale());
				}
				column.setDataType(exprName.getDataType());
			}else{
				exprName.setFrom(this, idx, column);
			}
		}else{
			patchExpressions(expr);
		}
		return expr;
	}

	
	
	final void execute() throws Exception{
        super.execute();
		from.execute();
		NextRow:
		while(from.next()){
			beforeFirst();
			while(next()){
				if(currentGroup == null || currentGroup.getBoolean()){
					accumulateRow();
					continue NextRow;
				}
			}
			// add a new row to the GroupResult
			addGroupRow();
			accumulateRow();
		}
		
		if(getRowCount() == 0 && groupBy == null){
			//special handling for SELECT count(*) FROM table
			//without GROUP BY and without any rows
			addGroupRow();
		}
		// reset the row counter
		beforeFirst();
	}
	
	
	
	/**
	 * Add a new Row to the MemoryResult. This occur because the 
	 * GROUP BY clause of the current row not relate to an exists row. 
	 *
	 */
	final private void addGroupRow(){
		// add a new row to the GroupResult
		ExpressionValue[] newRow = currentRow = new ExpressionValue[ expressions.size()];
		for(int i=0; i<newRow.length; i++){
			Expression expr = expressions.get(i);
			int type = expr.getType();
			if(type < Expression.GROUP_BEGIN) type = Expression.GROUP_BY; 
			newRow[i] = new ExpressionValue( type );
		}
		addRow(newRow);
	}
	
	
	final private void accumulateRow() throws Exception{
		for(int i=0; i<currentRow.length; i++){
			Expression src = expressions.get(i);
			currentRow[i].accumulate(src);
		}
	}
}
