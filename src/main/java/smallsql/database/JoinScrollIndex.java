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
 * JoinScrollIndex.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 10.07.2007
 */
package smallsql.database;

/**
 * @author Volker Berlin
 */
class JoinScrollIndex extends JoinScroll{

    private final int compare;

    Expressions leftEx;
    Expressions rightEx;

    private Index index;

    private LongTreeList rowList;

    private final LongTreeListEnum longListEnum = new LongTreeListEnum();


    JoinScrollIndex( int joinType, RowSource left, RowSource right, Expressions leftEx, Expressions rightEx, int compare)
            throws Exception{
        super( joinType, left, right, null);
        this.leftEx = leftEx;
        this.rightEx = rightEx;
        this.compare = compare;
        createIndex(rightEx);
    }


    private void createIndex(Expressions rightEx) throws Exception{
        index = new Index(false);
        right.beforeFirst();
        while(right.next()){
            index.addValues(right.getRowPosition(), rightEx);
        }
    }


    boolean next() throws Exception{
        switch(compare){
        case ExpressionArithmetic.EQUALS:
            return nextEquals();
        default:
            throw new Error("Compare operation not supported:" + compare);
        }

    }


    private boolean nextEquals() throws Exception{
        if(rowList != null){
            long rowPosition = rowList.getNext(longListEnum);
            if(rowPosition != -1){
                right.setRowPosition(rowPosition);
                return true;
            }
            rowList = null;
        }
        Object rows;
        do{
            if(!left.next()){
                return false;
            }
            rows = index.findRows(leftEx, false, null);
        }while(rows == null);
        
        if(rows instanceof Long){
            right.setRowPosition(((Long)rows).longValue());
        }else{
            rowList = (LongTreeList)rows;
            longListEnum.reset();
            right.setRowPosition(rowList.getNext(longListEnum));
        }
        return true;
    }


}
