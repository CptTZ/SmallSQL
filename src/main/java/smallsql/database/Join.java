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
 * Join.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.database;


final class Join extends RowSource{

    Expression condition; // the join condition, the part after the ON
    private int type;
    RowSource left; // the left table, view or rowsource of the join
    RowSource right;
	private boolean isAfterLast;
	
	
    private LongLongList rowPositions; // needed for getRowPosition() and setRowPosition()
    private int row; //current row number
    

    JoinScroll scroll;
    
    
    Join( int type, RowSource left, RowSource right, Expression condition ){
        this.type = type;
        this.condition = condition;
        this.left = left;
        this.right = right;
    }
    
    
	final boolean isScrollable(){
		return false; //TODO performance, if left and right are scrollable then this should also scrollable
	}

    
    void beforeFirst() throws Exception{
        scroll.beforeFirst();
		isAfterLast  = false;
		row = 0;
    }

    boolean first() throws Exception{
        beforeFirst();
        return next();
    }
    

    boolean next() throws Exception{
        if(isAfterLast) return false;
        row++;
        boolean result = scroll.next();
        if(!result){
            noRow();
        }
        return result;
    }
    
    
	void afterLast(){
		isAfterLast = true;
		noRow();
	}
	
    
	int getRow(){
		return row;
	}
	
    
	final long getRowPosition(){
		if(rowPositions == null) rowPositions = new LongLongList();
		rowPositions.add( left.getRowPosition(), right.getRowPosition());
		return rowPositions.size()-1;
	}
	
	final void setRowPosition(long rowPosition) throws Exception{
		left .setRowPosition( rowPositions.get1((int)rowPosition));
		right.setRowPosition( rowPositions.get2((int)rowPosition));
	}
	

	final boolean rowInserted(){
		return left.rowInserted() || right.rowInserted();
	}
	
	
	final boolean rowDeleted(){
		return left.rowDeleted() || right.rowDeleted();
	}
	
	
    /**
     * By OUTER or FULL JOIN must one rowsource set to null.
     */
    void nullRow(){
    	left.nullRow();
    	right.nullRow();
    	row = 0;
    }
    

	void noRow(){
		isAfterLast = true;
		left.noRow();
		right.noRow();
		row = 0;
	}

    
    void execute() throws Exception{
    	left.execute();
    	right.execute();
        //create the best join  algorithm
        if(!createJoinScrollIndex()){
            //Use the default join algorithm with a loop as fallback
            scroll = new JoinScroll(type, left, right, condition);
        }
    }
    
    
    /**
     * @inheritDoc
     */
    boolean isExpressionsFromThisRowSource(Expressions columns){
        if(left.isExpressionsFromThisRowSource(columns) || right.isExpressionsFromThisRowSource(columns)){
            return true;
        }
        if(columns.size() == 1){
            return false;
        }
        
        //Now it will be difficult, there are 2 or more column
        //one can in the left, the other can be in the right
        //Or one is not in both that we need to check everyone individually
        Expressions single = new Expressions();
        for(int i=0; i<columns.size(); i++){
            single.clear();
            single.add(columns.get(i));
            if(left.isExpressionsFromThisRowSource(columns) || right.isExpressionsFromThisRowSource(columns)){
                continue;
            }
            return false;
        }
        return true;
    }
    

    /**
     * Create a ScrollJoin that based on a index. 
     * There must not exist a index on a table. If there is no index then a index will be created.
     * @return null if it is not possible to create a ScrollJoin based on a Index
     */
    private boolean createJoinScrollIndex() throws Exception{
        if(type == CROSS_JOIN){
            return false;
        }
        if(type != INNER_JOIN){
            // TODO currently only INNER JOIN are implemented
            return false;
        }
        if(condition instanceof ExpressionArithmetic){
            ExpressionArithmetic cond = (ExpressionArithmetic)condition;
            Expressions leftEx = new Expressions();
            Expressions rightEx = new Expressions();
            int operation = createJoinScrollIndex(cond, leftEx, rightEx, 0);
            if(operation != 0){
                scroll = new JoinScrollIndex( type, left, right, leftEx, rightEx, operation);
                return true;
            }
        }
        return false;
    }
    
    
    private int createJoinScrollIndex(ExpressionArithmetic cond, Expressions leftEx, Expressions rightEx, int operation) throws Exception{
        Expression[] params = cond.getParams();
        int op = cond.getOperation();
        if(op == ExpressionArithmetic.AND){
            Expression param0 = params[0];
            Expression param1 = params[1];
            if(param0 instanceof ExpressionArithmetic && param1 instanceof ExpressionArithmetic){
                op = createJoinScrollIndex((ExpressionArithmetic)param0, leftEx, rightEx, operation);
                if(op == 0){
                    return 0;
                }
                return createJoinScrollIndex((ExpressionArithmetic)param1, leftEx, rightEx, operation);
            }
            return 0;
        }
        if(operation == 0){
            operation = op;
        }
        if(operation != op){
            return 0;
        }
        if(operation == ExpressionArithmetic.EQUALS){
            Expression param0 = params[0];
            Expression param1 = params[1];
            //scan all column that are include in the expression
            Expressions columns0 = Utils.getExpressionNameFromTree(param0);
            Expressions columns1 = Utils.getExpressionNameFromTree(param1);
            if(left.isExpressionsFromThisRowSource(columns0) && right.isExpressionsFromThisRowSource(columns1)){
                leftEx.add( param0 );
                rightEx.add( param1 );
            }else{
                if(left.isExpressionsFromThisRowSource(columns1) && right.isExpressionsFromThisRowSource(columns0)){
                    leftEx.add( param1 );
                    rightEx.add( param0 );
                }else{
                    return 0;
                }
            }
            
            return operation;
        }
        return 0;
    }
    
    
    static final int CROSS_JOIN = 1;
    static final int INNER_JOIN = 2;
    static final int LEFT_JOIN  = 3;
    static final int FULL_JOIN  = 4;
	static final int RIGHT_JOIN = 5;
}