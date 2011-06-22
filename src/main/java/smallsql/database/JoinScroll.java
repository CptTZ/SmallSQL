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
 * JoinScroll.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 12.07.2007
 */
package smallsql.database;



/**
 * @author Volker Berlin
 */
class JoinScroll{

    private final Expression condition; // the join condition, the part after the ON
    final int type;
    final RowSource left; // the left table, view or rowsource of the join
    final RowSource right;

    private boolean isBeforeFirst = true;
    private boolean isOuterValid = true;
    
    // Variables for FULL JOIN
    private boolean[] isFullNotValid;
    private int fullRightRowCounter;
    private int fullRowCount;
    private int fullReturnCounter = -1;
    
    
    JoinScroll( int type, RowSource left, RowSource right, Expression condition ){
        this.type = type;
        this.condition = condition;
        this.left = left;
        this.right = right;
        if(type == Join.FULL_JOIN){
            isFullNotValid = new boolean[10];
        }
    }
    
    
    void beforeFirst() throws Exception{
        left.beforeFirst();
        right.beforeFirst();
        isBeforeFirst = true;
        fullRightRowCounter = 0;
        fullRowCount        = 0;
        fullReturnCounter   = -1;
    }

    
    
    boolean next() throws Exception{
        boolean result;
        if(fullReturnCounter >=0){
            do{
                if(fullReturnCounter >= fullRowCount){
                    return false; 
                }
                right.next();
            }while(isFullNotValid[fullReturnCounter++]);
            return true;
        }
        do{
            if(isBeforeFirst){               
                result = left.next();
                if(result){ 
                    result = right.first();
                    if(!result){
                        switch(type){
                            case Join.LEFT_JOIN:
                            case Join.FULL_JOIN:
                                isOuterValid = false;
                                isBeforeFirst = false;
                                right.nullRow();
                                return true;
                        }
                    }else fullRightRowCounter++;
                }else{
                    // left does not include any row
                    if(type == Join.FULL_JOIN){
                        while(right.next()){
                            fullRightRowCounter++;
                        }
                        fullRowCount = fullRightRowCounter;
                    }
                }
            }else{
                result = right.next();              
                if(!result){
                    switch(type){
                        case Join.LEFT_JOIN:
                        case Join.FULL_JOIN:
                            if(isOuterValid){
                                isOuterValid = false;
                                right.nullRow();
                                return true;
                            }
                            fullRowCount = Math.max( fullRowCount, fullRightRowCounter);
                            fullRightRowCounter = 0;
                    }
                    isOuterValid = true;
                    result = left.next();
                    if(result){ 
                        result = right.first();
                        if(!result){
                            switch(type){
                                case Join.LEFT_JOIN:
                                case Join.FULL_JOIN:
                                    isOuterValid = false;
                                    right.nullRow();
                                    return true;
                            }
                        }else fullRightRowCounter++;
                    }
                    
                }else fullRightRowCounter++;
            }
            isBeforeFirst = false;
        }while(result && !getBoolean());
        isOuterValid = false;
        if(type == Join.FULL_JOIN){
            if(fullRightRowCounter >= isFullNotValid.length){
                boolean[] temp = new boolean[fullRightRowCounter << 1];
                System.arraycopy( isFullNotValid, 0, temp, 0, fullRightRowCounter);
                isFullNotValid = temp;
            }
            if(!result){
                if(fullRowCount == 0){
                    return false; 
                }
                if(fullReturnCounter<0) {
                    fullReturnCounter = 0;
                    right.first();
                    left.nullRow();
                }
                while(isFullNotValid[fullReturnCounter++]){
                    if(fullReturnCounter >= fullRowCount){
                       return false; 
                    }
                    right.next();
                }
                return true;
            }else
                isFullNotValid[fullRightRowCounter-1] = result;
        }
        return result;
    }

    
    private boolean getBoolean() throws Exception{
        return type == Join.CROSS_JOIN || condition.getBoolean();
    }
}
