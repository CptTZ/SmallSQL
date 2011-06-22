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
 * ExpressionArithmethic.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.database;

import java.sql.*;
import smallsql.database.language.Language;


public class ExpressionArithmetic extends Expression {

    private Expression left;
    private Expression right;
    private Expression right2;
    private Expression[] inList;
    final private int operation;

    /**
     * Constructor for NOT, NEGATIVE, BIT_NOT, ISNULL and ISNOTNULL
     */
    ExpressionArithmetic( Expression left, int operation){
    	super(FUNCTION);
        this.left  = left;
        this.operation = operation;
        super.setParams( new Expression[]{ left });
    }

    ExpressionArithmetic( Expression left, Expression right, int operation){
		super(FUNCTION);
        this.left   = left;
        this.right  = right;
        this.operation = operation;
        super.setParams( new Expression[]{ left, right });
    }

    /**
     * Constructor for BETWEEN
     */
    ExpressionArithmetic( Expression left, Expression right, Expression right2, int operation){
		super(FUNCTION);
        this.left   = left;
        this.right  = right;
        this.right2 = right2;
        this.operation = operation;
        super.setParams( new Expression[]{ left, right, right2 });
    }
    
    /**
     * Constructor for IN
     */
    ExpressionArithmetic( Expression left, Expressions inList, int operation){
		super(FUNCTION);
        this.left   = left;
        this.operation = operation;
		Expression[] params;
        if(inList != null){
	        this.inList = inList.toArray();
	        params = new Expression[this.inList.length+1];
	        params[0] = left;
	        System.arraycopy(this.inList, 0, params, 1, this.inList.length);
        }else{
            //Occur with ExpressionInSelect, in this case the method isInList() is overridden
			params = new Expression[]{ left };
        }
        super.setParams( params );
    }
    
    
    /**
     * Get the arithmetic operation of this expression.
     * @return
     */
    int getOperation(){
        return operation;
    }
      
    
    private Expression convertExpressionIfNeeded( Expression expr, Expression other ){
        if(expr == null || other == null){
            return expr;
        }
        switch(expr.getDataType()){
        case SQLTokenizer.CHAR:
        case SQLTokenizer.NCHAR:
        case SQLTokenizer.BINARY:
            switch(other.getDataType()){
            case SQLTokenizer.VARCHAR:
            case SQLTokenizer.NVARCHAR:
            case SQLTokenizer.CLOB:
            case SQLTokenizer.NCLOB:
            case SQLTokenizer.LONGNVARCHAR:
            case SQLTokenizer.LONGVARCHAR:
            case SQLTokenizer.VARBINARY:
                ExpressionFunctionRTrim trim = new ExpressionFunctionRTrim();
                trim.setParams(new Expression[]{expr});
                return trim;
            case SQLTokenizer.CHAR:
            case SQLTokenizer.NCHAR:
            case SQLTokenizer.BINARY:
                if(other.getPrecision() > expr.getPrecision()){
                    return new ExpressionFunctionConvert(new ColumnExpression(other), expr, null );
                }
                break; 
            }
            break;
        }
        return expr;
    }
    

	final void setParamAt( Expression param, int idx){
		switch(idx){
			case 0:
				left = param;
				break;
			case 1:
                if(right != null){
                    right = param;
                }
				break;
			case 2:
                if(right != null){
                    right2 = param;
                }
				break;
		}
		if(inList != null && idx>0 && idx<=inList.length){
			inList[idx-1] = param;
		}
		super.setParamAt( param, idx );
	}


	/**
	 * Is used in GroupResult.
	 */
	public boolean equals(Object expr){
		if(!super.equals(expr)) return false;
		if(!(expr instanceof ExpressionArithmetic)) return false;
		if( ((ExpressionArithmetic)expr).operation != operation) return false;
		return true;
	}


	
    int getInt() throws java.lang.Exception {
        if(isNull()) return 0;
        int dataType = getDataType();
        switch(dataType){
            case SQLTokenizer.BIT:
            case SQLTokenizer.BOOLEAN:
				return getBoolean() ? 1 : 0;
            case SQLTokenizer.TINYINT:
            case SQLTokenizer.SMALLINT:
            case SQLTokenizer.INT:
				return getIntImpl();
            case SQLTokenizer.BIGINT:
                return (int)getLongImpl();
			case SQLTokenizer.REAL:
                return (int)getFloatImpl();
			case SQLTokenizer.FLOAT:
			case SQLTokenizer.DOUBLE:
            case SQLTokenizer.MONEY:
            case SQLTokenizer.SMALLMONEY:
            case SQLTokenizer.NUMERIC:
            case SQLTokenizer.DECIMAL:
            	// FIXME: bug! if get returns a number outside of
            	// integer interval, it's not rounded to max/min, 
            	// instead it returns a wrong value
                return (int)getDoubleImpl();
        }
        throw createUnspportedConversion( SQLTokenizer.INT);
    }
    
    
    private int getIntImpl() throws java.lang.Exception {
        switch(operation){
            case ADD:       return left.getInt() + right.getInt();
            case SUB:       return left.getInt() - right.getInt();
            case MUL:       return left.getInt() * right.getInt();
            case DIV:       return left.getInt() / right.getInt();
            case NEGATIVE:  return               - left.getInt();
            case MOD:		return left.getInt() % right.getInt();
            case BIT_NOT:   return               ~ left.getInt();
        }
        throw createUnspportedConversion( SQLTokenizer.INT);
    }
    
    
	long getLong() throws java.lang.Exception {
        if(isNull()) return 0;
        int dataType = getDataType();
        switch(dataType){
            case SQLTokenizer.BIT:
            case SQLTokenizer.BOOLEAN:
				return getBoolean() ? 1 : 0;
            case SQLTokenizer.TINYINT:
            case SQLTokenizer.SMALLINT:
            case SQLTokenizer.INT:
				return getIntImpl();
            case SQLTokenizer.BIGINT:
                return getLongImpl();
			case SQLTokenizer.REAL:
                return (long)getFloatImpl();
			case SQLTokenizer.FLOAT:
			case SQLTokenizer.DOUBLE:
            case SQLTokenizer.MONEY:
            case SQLTokenizer.SMALLMONEY:
            case SQLTokenizer.NUMERIC:
            case SQLTokenizer.DECIMAL:
                return (long)getDoubleImpl();
        }
		throw createUnspportedConversion( SQLTokenizer.LONG);
    }
	
	
	private long getLongImpl() throws java.lang.Exception {
        if(isNull()) return 0;
        switch(operation){
            case ADD: return left.getLong() + right.getLong();
            case SUB: return left.getLong() - right.getLong();
            case MUL: return left.getLong() * right.getLong();
            case DIV: return left.getLong() / right.getLong();
            case NEGATIVE:  return          - left.getLong();
            case MOD:		return left.getLong() % right.getLong();
            case BIT_NOT:   return          ~ right.getInt();
        }
		throw createUnspportedConversion( SQLTokenizer.LONG);
    }
	
	
    double getDouble() throws java.lang.Exception {
        if(isNull()) return 0;
        int dataType = getDataType();
        switch(dataType){
            case SQLTokenizer.BIT:
            case SQLTokenizer.BOOLEAN:
				return getBoolean() ? 1 : 0;
            case SQLTokenizer.TINYINT:
            case SQLTokenizer.SMALLINT:
            case SQLTokenizer.INT:
				return getIntImpl();
            case SQLTokenizer.BIGINT:
                return getLongImpl();
			case SQLTokenizer.REAL:
                return getFloatImpl();
			case SQLTokenizer.FLOAT:
			case SQLTokenizer.DOUBLE:
            case SQLTokenizer.MONEY:
            case SQLTokenizer.SMALLMONEY:
            case SQLTokenizer.NUMERIC:
            case SQLTokenizer.DECIMAL:
                return getDoubleImpl();
        }
		throw createUnspportedConversion( SQLTokenizer.DOUBLE);
    }
	
	
    private double getDoubleImpl() throws java.lang.Exception{
		if(operation == NEGATIVE)
			return getDoubleImpl(0, left.getDouble());
		return getDoubleImpl(left.getDouble(), right.getDouble());
	}
	
	
    private double getDoubleImpl( double lVal, double rVal) throws java.lang.Exception{
        switch(operation){
            case ADD: return lVal + rVal;
            case SUB: return lVal - rVal;
            case MUL: return lVal * rVal;
            case DIV: return lVal / rVal;
            case NEGATIVE: return - rVal;
            case MOD:		return lVal % rVal;
        }
        throw createUnspportedConversion( SQLTokenizer.DOUBLE);
    }
	

    float getFloat() throws java.lang.Exception {
        if(isNull()) return 0;
        int dataType = getDataType();
        switch(dataType){
            case SQLTokenizer.BIT:
            case SQLTokenizer.BOOLEAN:
				return getBoolean() ? 1 : 0;
            case SQLTokenizer.TINYINT:
            case SQLTokenizer.SMALLINT:
            case SQLTokenizer.INT:
				return getIntImpl();
            case SQLTokenizer.BIGINT:
                return getLongImpl();
			case SQLTokenizer.REAL:
                return getFloatImpl();
			case SQLTokenizer.FLOAT:
			case SQLTokenizer.DOUBLE:
            case SQLTokenizer.MONEY:
            case SQLTokenizer.SMALLMONEY:
            case SQLTokenizer.NUMERIC:
            case SQLTokenizer.DECIMAL:
                return (float)getDoubleImpl();
        }
		throw createUnspportedConversion( SQLTokenizer.DOUBLE);
    }
    
    
    private float getFloatImpl() throws java.lang.Exception {
        switch(operation){
            case ADD: return left.getFloat() + right.getFloat();
            case SUB: return left.getFloat() - right.getFloat();
            case MUL: return left.getFloat() * right.getFloat();
            case DIV: return left.getFloat() / right.getFloat();
            case NEGATIVE:  return           - left.getFloat();
            case MOD:		return left.getFloat() % right.getFloat();
        }
        throw createUnspportedConversion( SQLTokenizer.REAL );
    }
    
    
    long getMoney() throws java.lang.Exception {
        if(isNull()) return 0;
        int dataType = getDataType();		
        switch(dataType){
            case SQLTokenizer.BIT:
            case SQLTokenizer.BOOLEAN:
				return getBoolean() ? 10000 : 0;
            case SQLTokenizer.TINYINT:
            case SQLTokenizer.SMALLINT:
            case SQLTokenizer.INT:
				return getIntImpl() * 10000;
            case SQLTokenizer.BIGINT:
                return getLongImpl() * 10000;
			case SQLTokenizer.REAL:
                return Utils.doubleToMoney( getFloatImpl() );
			case SQLTokenizer.FLOAT:
			case SQLTokenizer.DOUBLE:
            case SQLTokenizer.NUMERIC:
            case SQLTokenizer.DECIMAL:
                return Utils.doubleToMoney( getDoubleImpl() );
            case SQLTokenizer.MONEY:
            case SQLTokenizer.SMALLMONEY:
				return getMoneyImpl();
        }
		throw createUnspportedConversion( SQLTokenizer.DOUBLE);
    }
    

    private long getMoneyImpl() throws java.lang.Exception {
        switch(operation){
            case ADD: return left.getMoney() + right.getMoney();
            case SUB: return left.getMoney() - right.getMoney();
            case MUL: return left.getMoney() * right.getMoney() / 10000;
            case DIV: return left.getMoney() * 10000 / right.getMoney();					
            case NEGATIVE: return 			 - left.getMoney();
        }
        throw createUnspportedConversion( SQLTokenizer.MONEY );
    }
    

    MutableNumeric getNumeric() throws java.lang.Exception {
        if(isNull()) return null;
        int dataType = getDataType();		
        switch(dataType){
            case SQLTokenizer.BIT:
            case SQLTokenizer.BOOLEAN:
				return new MutableNumeric(getBoolean() ? 1 : 0);
            case SQLTokenizer.TINYINT:
            case SQLTokenizer.SMALLINT:
            case SQLTokenizer.INT:
				return new MutableNumeric(getIntImpl());
            case SQLTokenizer.BIGINT:
                return new MutableNumeric(getLongImpl());
			case SQLTokenizer.REAL:
                return new MutableNumeric(getFloatImpl());
			case SQLTokenizer.FLOAT:
			case SQLTokenizer.DOUBLE:
                return new MutableNumeric( getDoubleImpl() );
            case SQLTokenizer.NUMERIC:
            case SQLTokenizer.DECIMAL:
                return getNumericImpl();
            case SQLTokenizer.MONEY:
            case SQLTokenizer.SMALLMONEY:
				return new MutableNumeric(getMoneyImpl(),4);
        }
		throw createUnspportedConversion( SQLTokenizer.DOUBLE);
    }
    
    
    private MutableNumeric getNumericImpl() throws java.lang.Exception {
        switch(operation){
            case ADD: 
            	{
					MutableNumeric num = left.getNumeric();
            		num.add( right.getNumeric() );
            		return num;
            	}
            case SUB:
				{
					MutableNumeric num = left.getNumeric();
					num.sub( right.getNumeric() );
					return num;
				}
            case MUL: 
            	if(getDataType(right.getDataType(), SQLTokenizer.INT) == SQLTokenizer.INT){
            		MutableNumeric num = left.getNumeric();
            		num.mul(right.getInt());
            		return num;
            	}else
            	if(getDataType(left.getDataType(), SQLTokenizer.INT) == SQLTokenizer.INT){
					MutableNumeric num = right.getNumeric();
					num.mul(left.getInt());
					return num;
            	}else{
					MutableNumeric num = left.getNumeric();
					num.mul( right.getNumeric() );
					return num;
            	}
            case DIV:
            	{
					MutableNumeric num = left.getNumeric();
            		if(getDataType(right.getDataType(), SQLTokenizer.INT) == SQLTokenizer.INT)
            			num.div( right.getInt() );
            		else
            			num.div( right.getNumeric() ); 
            		return num;
            	}
            case NEGATIVE:
            	{
					MutableNumeric num = left.getNumeric();
					num.setSignum(-num.getSignum());
					return num;
            	}
            case MOD:
				{
					if(getDataType(getDataType(), SQLTokenizer.INT) == SQLTokenizer.INT)
						return new MutableNumeric(getInt());
					MutableNumeric num = left.getNumeric();
					num.mod( right.getNumeric() );
					return num;
				}
            default:    throw createUnspportedConversion( SQLTokenizer.NUMERIC );
        }
    }
    
    
    Object getObject() throws java.lang.Exception {
        if(isNull()) return null;
        int dataType = getDataType();
        switch(dataType){
            case SQLTokenizer.BIT:
            case SQLTokenizer.BOOLEAN:
                    return getBoolean() ? Boolean.TRUE : Boolean.FALSE;
            case SQLTokenizer.BINARY:
            case SQLTokenizer.VARBINARY:
                    return getBytes();
            case SQLTokenizer.TINYINT:
            case SQLTokenizer.SMALLINT:
            case SQLTokenizer.INT:
                    return new Integer( getInt() );
            case SQLTokenizer.BIGINT:
                    return new Long( getLong() );
            case SQLTokenizer.REAL:
                    return new Float( getFloat() );
            case SQLTokenizer.FLOAT:
            case SQLTokenizer.DOUBLE:
                    return new Double( getDouble() );
            case SQLTokenizer.MONEY:
            case SQLTokenizer.SMALLMONEY:
                    return Money.createFromUnscaledValue( getMoney() );
            case SQLTokenizer.NUMERIC:
            case SQLTokenizer.DECIMAL:
                    return getNumeric();
            case SQLTokenizer.CHAR:
            case SQLTokenizer.NCHAR:
            case SQLTokenizer.VARCHAR:
            case SQLTokenizer.NVARCHAR:
            case SQLTokenizer.LONGNVARCHAR:
            case SQLTokenizer.LONGVARCHAR:
            		return getString( left.getString(), right.getString() );
            case SQLTokenizer.JAVA_OBJECT:
                    Object lObj = left.getObject();
                    //FIXME NullPointerException bei NEGATIVE
                    Object rObj = right.getObject();
                    if(lObj instanceof Number && rObj instanceof Number)
                        return new Double( getDoubleImpl( ((Number)lObj).doubleValue(), ((Number)rObj).doubleValue() ) );
                    else
                        return getString( lObj.toString(), rObj.toString() );
            case SQLTokenizer.LONGVARBINARY:
                    return getBytes();
			case SQLTokenizer.DATE:
			case SQLTokenizer.TIME:
			case SQLTokenizer.TIMESTAMP:
			case SQLTokenizer.SMALLDATETIME:
				return new DateTime( getLong(), dataType );
            case SQLTokenizer.UNIQUEIDENTIFIER:
                    return getBytes();
            default: throw createUnspportedDataType();
        }
    }
    
    
    boolean getBoolean() throws java.lang.Exception {
        switch(operation){
        	case OR:    return left.getBoolean() || right.getBoolean();
            case AND:   return left.getBoolean() && right.getBoolean();
            case NOT:   return                      !left.getBoolean();
            case LIKE:  return Utils.like( left.getString(), right.getString());
            case ISNULL:return 						left.isNull();
			case ISNOTNULL:	return 					!left.isNull();
			case IN:	if(right == null)
							return isInList();
						break;
        }
        final boolean leftIsNull = left.isNull();
        int dataType;
        if(operation == NEGATIVE || operation == BIT_NOT){
        	if(leftIsNull) return false;
        	dataType = left.getDataType();
        }else{
            final boolean rightIsNull = right.isNull();
        	if(operation == EQUALS_NULL && leftIsNull && rightIsNull) return true;
        	if(leftIsNull || rightIsNull) return false;
        	dataType = getDataType(left, right);
        }
        switch(dataType){
			case SQLTokenizer.BOOLEAN:
					switch(operation){
						case IN:
						case EQUALS_NULL:
						case EQUALS:    return left.getBoolean() == right.getBoolean();
						case UNEQUALS:  return left.getBoolean() != right.getBoolean();
					}
					//break; interpret it as BIT 
            case SQLTokenizer.TINYINT:
            case SQLTokenizer.SMALLINT:
            case SQLTokenizer.INT:
            case SQLTokenizer.BIT:
                    switch(operation){
						case IN:
						case EQUALS_NULL:
                        case EQUALS:    return left.getInt() == right.getInt();
                        case GREATER:   return left.getInt() >  right.getInt();
                        case GRE_EQU:   return left.getInt() >= right.getInt();
                        case LESSER:    return left.getInt() <  right.getInt();
                        case LES_EQU:   return left.getInt() <= right.getInt();
                        case UNEQUALS:  return left.getInt() != right.getInt();
                        case BETWEEN:
                                        int _left = left.getInt();
                                        return _left >= right.getInt() && right2.getInt() >= _left;
                        default:
                        	return getInt() != 0;
                    }
            case SQLTokenizer.BIGINT:
			case SQLTokenizer.TIMESTAMP:
			case SQLTokenizer.TIME:
			case SQLTokenizer.DATE:
			case SQLTokenizer.SMALLDATETIME:
                    switch(operation){
						case IN:
						case EQUALS_NULL:
                        case EQUALS:    return left.getLong() == right.getLong();
                        case GREATER:   return left.getLong() >  right.getLong();
                        case GRE_EQU:   return left.getLong() >= right.getLong();
                        case LESSER:    return left.getLong() <  right.getLong();
                        case LES_EQU:   return left.getLong() <= right.getLong();
                        case UNEQUALS:  return left.getLong() != right.getLong();
                        case BETWEEN:
                                        long _left = left.getLong();
                                        return _left >= right.getLong() && right2.getLong() >= _left;
                        default:
                        	return getLong() != 0;
                    }
            case SQLTokenizer.REAL:
                    switch(operation){
						case IN:
						case EQUALS_NULL:
                        case EQUALS:    return left.getFloat() == right.getFloat();
                        case GREATER:   return left.getFloat() >  right.getFloat();
                        case GRE_EQU:   return left.getFloat() >= right.getFloat();
                        case LESSER:    return left.getFloat() <  right.getFloat();
                        case LES_EQU:   return left.getFloat() <= right.getFloat();
                        case UNEQUALS:  return left.getFloat() != right.getFloat();
                        case BETWEEN:
                                        float _left = left.getFloat();
                                        return _left >= right.getFloat() && right2.getFloat() >= _left;
                        default:
                        	return getFloat() != 0;
                    }
            case SQLTokenizer.FLOAT:
            case SQLTokenizer.DOUBLE:
                    switch(operation){
						case IN:
						case EQUALS_NULL:
                        case EQUALS:    return left.getDouble() == right.getDouble();
                        case GREATER:   return left.getDouble() >  right.getDouble();
                        case GRE_EQU:   return left.getDouble() >= right.getDouble();
                        case LESSER:    return left.getDouble() <  right.getDouble();
                        case LES_EQU:   return left.getDouble() <= right.getDouble();
                        case UNEQUALS:  return left.getDouble() != right.getDouble();
                        case BETWEEN:
                                        double _left = left.getDouble();
                                        return _left >= right.getDouble() && right2.getDouble() >= _left;
                        default:
                        	return getDouble() != 0;
                    }
            case SQLTokenizer.MONEY:
            case SQLTokenizer.SMALLMONEY:
                    switch(operation){
						case IN:
						case EQUALS_NULL:
                        case EQUALS:    return left.getMoney() == right.getMoney();
                        case GREATER:   return left.getMoney() >  right.getMoney();
                        case GRE_EQU:   return left.getMoney() >= right.getMoney();
                        case LESSER:    return left.getMoney() <  right.getMoney();
                        case LES_EQU:   return left.getMoney() <= right.getMoney();
                        case UNEQUALS:  return left.getMoney() != right.getMoney();
                        case BETWEEN:
                                        long _left = left.getMoney();
                                        return _left >= right.getMoney() && right2.getMoney() >= _left;
                        default:
                        	return getMoney() != 0;
                    }
            case SQLTokenizer.DECIMAL:
			case SQLTokenizer.NUMERIC:{
					if(operation == NEGATIVE)
						return left.getNumeric().getSignum() != 0;
					int comp = left.getNumeric().compareTo( right.getNumeric() );
					switch(operation){
						case IN:
						case EQUALS_NULL:
						case EQUALS:    return comp == 0;
						case GREATER:   return comp >  0;
						case GRE_EQU:   return comp >= 0;
						case LESSER:    return comp <  0;
						case LES_EQU:   return comp <= 0;
						case UNEQUALS:  return comp != 0;
						case BETWEEN:
										return comp >= 0 && 0 >= left.getNumeric().compareTo( right2.getNumeric() );
                        default:
                        	return getNumeric().getSignum() != 0;
					}
					}
            case SQLTokenizer.VARCHAR:
            case SQLTokenizer.NVARCHAR:
            case SQLTokenizer.CHAR:
            case SQLTokenizer.NCHAR:
            case SQLTokenizer.LONGVARCHAR:
            case SQLTokenizer.LONGNVARCHAR:
            case SQLTokenizer.CLOB:{
                    final String leftStr = left.getString();
                    final String rightStr = right.getString();
                    int comp = String.CASE_INSENSITIVE_ORDER.compare( leftStr, rightStr );
                    switch(operation){
						case IN:
						case EQUALS_NULL:
                        case EQUALS:    return comp == 0;
                        case GREATER:   return comp >  0;
                        case GRE_EQU:   return comp >= 0;
                        case LESSER:    return comp <  0;
                        case LES_EQU:   return comp <= 0;
                        case UNEQUALS:  return comp != 0;
                        case BETWEEN:
                                        return comp >= 0 && 0 >= String.CASE_INSENSITIVE_ORDER.compare( leftStr, right2.getString() );
                        case ADD:       return Utils.string2boolean(leftStr + rightStr);
                    }
                    break;}
            case SQLTokenizer.BINARY:
            case SQLTokenizer.VARBINARY:
            case SQLTokenizer.LONGVARBINARY:
            case SQLTokenizer.BLOB:
			case SQLTokenizer.UNIQUEIDENTIFIER:{
                    byte[] leftBytes = left.getBytes();
                    byte[] rightBytes= right.getBytes();
                    int comp = Utils.compareBytes( leftBytes, rightBytes);
                    switch(operation){
						case IN:
						case EQUALS_NULL:
                        case EQUALS:    return comp == 0;
                        case GREATER:   return comp >  0;
                        case GRE_EQU:   return comp >= 0;
                        case LESSER:    return comp <  0;
                        case LES_EQU:   return comp <= 0;
                        case UNEQUALS:  return comp != 0;
                        case BETWEEN:
                                        return comp >= 0 && 0 >= Utils.compareBytes( leftBytes, right2.getBytes() );
                    }
                    break;}
        }
        throw createUnspportedDataType();
    }
    
    
    String getString() throws java.lang.Exception {
        if(isNull()) return null;
        return getObject().toString();
    }
    
    
    final private String getString( String lVal, String rVal ) throws java.lang.Exception {
        switch(operation){
            case ADD: return lVal + rVal;
        }
        throw createUnspportedConversion( SQLTokenizer.VARCHAR );
    }

    
    int getDataType() {
        switch(operation){
            case NEGATIVE:
            case BIT_NOT:
            	return left.getDataType();
			case EQUALS:
			case EQUALS_NULL:
			case GREATER:
			case GRE_EQU:
			case LESSER:
			case LES_EQU:
			case UNEQUALS:
			case BETWEEN:
			case OR:
			case AND:
			case NOT:
			case LIKE:
			case ISNULL:
			case ISNOTNULL:
			 	return SQLTokenizer.BOOLEAN;
            default:
            	return getDataType(left, right);
        }
    }
	
	
	int getScale(){
		int dataType = getDataType();
		switch(dataType){
			case SQLTokenizer.DECIMAL:
			case SQLTokenizer.NUMERIC:
				switch(operation){
					case ADD:
					case SUB:
						return Math.max(left.getScale(), right.getScale());
					case MUL:
						return left.getScale() + right.getScale();
					case DIV:
						return Math.max(left.getScale()+5, right.getScale()+4);
					case NEGATIVE:
						return left.getScale();
					case MOD:
						return 0;
				}
		}
		return getScale(dataType);
	}

    
    boolean isNull() throws Exception{
        switch(operation){
	        case OR:
	        case AND:
	        case NOT:
	        case LIKE:
	        case ISNULL:
			case ISNOTNULL:
			case IN:
							return false; //Boolean operations return ever a result ???, but at least ISNULL and ISNOTNULL
            case NEGATIVE: 
            case BIT_NOT:
                           return                  left.isNull();
            default:       return left.isNull() || right.isNull();
        }
    }


    byte[] getBytes() throws java.lang.Exception {
        throw createUnspportedConversion( SQLTokenizer.BINARY );
    }
    
    
    boolean isInList() throws Exception{
    	if(left.isNull()) return false;
    	try{
	    	for(int i=0; i<inList.length; i++){
	    		right = inList[i];
	    		if(getBoolean()) return true;
	    	}
    	}finally{
    		right = null;
    	}
    	return false;
    }

    
    SQLException createUnspportedDataType(){
    	Object[] params = {
    			SQLTokenizer.getKeyWord(getDataType(left, right)),
    			getKeywordFromOperation(operation)
    	};
        return SmallSQLException.create(Language.UNSUPPORTED_DATATYPE_OPER, params);
    }

    
    SQLException createUnspportedConversion( int dataType ){
        int type = left == null ? right.getDataType() : getDataType(left, right);
        Object[] params = new Object[] {
        		SQLTokenizer.getKeyWord(dataType),
        		SQLTokenizer.getKeyWord(type),
        		getKeywordFromOperation(operation)
        };
        return SmallSQLException.create(Language.UNSUPPORTED_CONVERSION_OPER, params);
    }
    
    
    void optimize() throws SQLException{
        super.optimize();
        Expression[] params = getParams();
        if(params.length == 1){
            return;
        }
        setParamAt( convertExpressionIfNeeded( params[0], params[1] ), 0 );
        
        for(int p=1; p<params.length; p++){
            setParamAt( convertExpressionIfNeeded( params[p], left ), p );
        }
    }
    
    /**
     * This method only for creating an error message. Thats there is no optimizing.
     * @param value
     * @return
     */
    private static String getKeywordFromOperation(int operation){
    	int token = 0;
    	for(int i=1; i<1000; i++){
    		if(getOperationFromToken(i) == operation){
				token = i;
				break;
    		}
    	}
    	if(operation == NEGATIVE)  token = SQLTokenizer.MINUS;
    	if(operation == ISNOTNULL) token =  SQLTokenizer.IS;
    	String keyword = SQLTokenizer.getKeyWord(token);
    	if(keyword == null) keyword = "" + (char)token;
    	return keyword;
    }

    
    static int getOperationFromToken( int value ){
        switch(value){
            case SQLTokenizer.PLUS:         return ADD;
            case SQLTokenizer.MINUS:        return SUB;
            case SQLTokenizer.ASTERISK:     return MUL;
            case SQLTokenizer.SLACH:        return DIV;
            case SQLTokenizer.PERCENT:      return MOD;
            case SQLTokenizer.EQUALS:       return EQUALS;
            case SQLTokenizer.GREATER:      return GREATER;
            case SQLTokenizer.GREATER_EQU:  return GRE_EQU;
            case SQLTokenizer.LESSER:       return LESSER;
            case SQLTokenizer.LESSER_EQU:   return LES_EQU;
            case SQLTokenizer.UNEQUALS:     return UNEQUALS;
            case SQLTokenizer.BETWEEN:      return BETWEEN;
            case SQLTokenizer.LIKE:         return LIKE;
            case SQLTokenizer.IN:           return IN;
			case SQLTokenizer.IS:           return ISNULL;
            case SQLTokenizer.OR:           return OR;
            case SQLTokenizer.AND:          return AND;
            case SQLTokenizer.NOT:          return NOT;
            case SQLTokenizer.BIT_OR:       return BIT_OR;
            case SQLTokenizer.BIT_AND:      return BIT_AND;
            case SQLTokenizer.BIT_XOR:      return BIT_XOR;
            case SQLTokenizer.TILDE:        return BIT_NOT;
            default:                        return 0;
        }
    }
    
    
	/**
	 * Returns the higher level data type from 2 expressions. 
	 */
    static int getDataType(Expression left, Expression right){
		int typeLeft  = left.getDataType();
		int typeRight = right.getDataType();
		return getDataType( typeLeft, typeRight);
    }
    

	/**
	 * Return the best data type for a complex number operation. This method return only 
	 * SQLTokenizer.INT,
	 * SQLTokenizer.BIGINT,
	 * SQLTokenizer.MONEY,
	 * SQLTokenizer.DECIMAL or
	 * SQLTokenizer.DOUBLE.
	 * @param paramDataType
	 */
	static int getBestNumberDataType(int paramDataType){
		int dataTypeIdx = Utils.indexOf( paramDataType, DatatypeRange);
		if(dataTypeIdx >= NVARCHAR_IDX)
			return SQLTokenizer.DOUBLE;
		if(dataTypeIdx >= INT_IDX)
			return SQLTokenizer.INT;
		if(dataTypeIdx >= BIGINT_IDX)
			return SQLTokenizer.BIGINT;
		if(dataTypeIdx >= MONEY_IDX)
			return SQLTokenizer.MONEY;
		if(dataTypeIdx >= DECIMAL_IDX)
			return SQLTokenizer.DECIMAL;
		return SQLTokenizer.DOUBLE;
	}
	
    /**
     * Returns the higher level data type from 2 data types. 
     */
	static int getDataType(int typeLeft, int typeRight){
		if(typeLeft == typeRight) return typeLeft;

		int dataTypeIdx = Math.min( Utils.indexOf( typeLeft, DatatypeRange), Utils.indexOf( typeRight, DatatypeRange) );
		if(dataTypeIdx < 0) throw new Error("getDataType(): "+typeLeft+", "+typeRight);
		return DatatypeRange[ dataTypeIdx ];
    }
	

    // value decade is the operation order
    static final int OR         = 11; // OR
    static final int AND        = 21; // AND
    static final int NOT        = 31; // NOT
    static final int BIT_OR     = 41; // |
    static final int BIT_AND    = 42; // &
    static final int BIT_XOR    = 43; // ^
    static final int EQUALS     = 51; // =
	static final int EQUALS_NULL= 52; // like Equals but (null = null) --> true 
    static final int GREATER    = 53; // >
    static final int GRE_EQU    = 54; // >=
    static final int LESSER     = 55; // <
    static final int LES_EQU    = 56; // <=
    static final int UNEQUALS   = 57; // <>
	static final int IN         = 61; // IN
	static final int BETWEEN    = 62; // BETWEEN
	static final int LIKE       = 63; // LIKE
	static final int ISNULL     = 64; // IS NULL
	static final int ISNOTNULL  = ISNULL+1; // IS NOT NULL 
    static final int ADD        = 71; // +
    static final int SUB        = 72; // -
    static final int MUL        = 81; // *
    static final int DIV        = 82; // /
    static final int MOD        = 83; // %
    static final int BIT_NOT    = 91; // ~
    static final int NEGATIVE   =101; // -

    private static final int[] DatatypeRange = {
        SQLTokenizer.TIMESTAMP,
        SQLTokenizer.SMALLDATETIME,
		SQLTokenizer.DATE,
		SQLTokenizer.TIME,
        SQLTokenizer.DOUBLE,
        SQLTokenizer.FLOAT,
        SQLTokenizer.REAL,
        SQLTokenizer.DECIMAL,
        SQLTokenizer.NUMERIC,
        SQLTokenizer.MONEY,
        SQLTokenizer.SMALLMONEY,
        SQLTokenizer.BIGINT,
        SQLTokenizer.INT,
        SQLTokenizer.SMALLINT,
        SQLTokenizer.TINYINT,
        SQLTokenizer.BIT,
        SQLTokenizer.BOOLEAN,
        SQLTokenizer.LONGNVARCHAR,
        SQLTokenizer.UNIQUEIDENTIFIER,
        SQLTokenizer.NVARCHAR,
        SQLTokenizer.NCHAR,
        SQLTokenizer.VARCHAR,
        SQLTokenizer.CHAR,
		SQLTokenizer.LONGVARCHAR,
        SQLTokenizer.CLOB,
        SQLTokenizer.VARBINARY,
        SQLTokenizer.BINARY,
        SQLTokenizer.LONGVARBINARY,
        SQLTokenizer.BLOB,
    	SQLTokenizer.NULL};

	
	private static int NVARCHAR_IDX = Utils.indexOf( SQLTokenizer.NVARCHAR, DatatypeRange);
	private static int INT_IDX = Utils.indexOf( SQLTokenizer.INT, DatatypeRange);
	private static int BIGINT_IDX = Utils.indexOf( SQLTokenizer.BIGINT, DatatypeRange);
	private static int MONEY_IDX = Utils.indexOf( SQLTokenizer.MONEY, DatatypeRange);
	private static int DECIMAL_IDX = Utils.indexOf( SQLTokenizer.DECIMAL, DatatypeRange);
}