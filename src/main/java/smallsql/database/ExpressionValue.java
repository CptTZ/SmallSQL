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
 * ExpressionValue.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.database;

import java.math.BigDecimal;
import java.sql.*;
import smallsql.database.language.Language;

public class ExpressionValue extends Expression {

    private Object value;
    private int dataType;
	private int length;

    /**
     * Constructor is used from PreparedStatement parameters ( '?' in sql expression )
     */
    ExpressionValue(){
		super(VALUE);
        clear();
    }

	/**
	 * Constructor is used from Constructor GroupResult
	 */
	ExpressionValue(int type){
		super(type);
		switch(type){
			case GROUP_BY:
			case SUM:
			case FIRST:
			case LAST:
				clear();
				break;
			case MIN:
			case MAX:
				// set value to null
				break;
			case COUNT:
				value = new MutableInteger(0);
				dataType = SQLTokenizer.INT;
				break;
			default: throw new Error();
		}
	}
	

    /**
     * Constructor for static Expression i.e. 0x23, 67, 23.8, 'qwert'
     */
    ExpressionValue(Object value, int dataType ){
		super(VALUE);
        this.value      = value;
        this.dataType   = dataType;
    }
    

	/**
	 * Is used in GroupResult.
	 */
	public boolean equals(Object expr){
		if(!super.equals(expr)) return false;
		if(!(expr instanceof ExpressionValue)) return false;
		Object v = ((ExpressionValue)expr).value;
		if(v == value) return true;
		if(value == null) return false;
		return value.equals(v);
	}

	
/*==============================================================================
methods for Grouping
==============================================================================*/
	/**
	 * Accumulate the value of the expression to this aggregate function value. 
	 */
    void accumulate(Expression expr) throws Exception{
		int type = getType();
		if(type != GROUP_BY) expr = expr.getParams()[0];
		switch(type){
			case GROUP_BY:
			case FIRST:
				if(isEmpty()) set( expr.getObject(), expr.getDataType() );
				break;
			case LAST:
				set( expr.getObject(), expr.getDataType() );
				break;
			case COUNT:
				if(!expr.isNull()) ((MutableInteger)value).value++;
				break;
			case SUM:
				if(isEmpty()){
					initValue( expr );
				}else
				switch(dataType){
					case SQLTokenizer.TINYINT:
					case SQLTokenizer.SMALLINT:
					case SQLTokenizer.INT:
						((MutableInteger)value).value += expr.getInt();
						break;
					case SQLTokenizer.BIGINT:
						((MutableLong)value).value += expr.getLong();
						break;
					case SQLTokenizer.REAL:
						((MutableFloat)value).value += expr.getFloat();
						break;
					case SQLTokenizer.FLOAT:
					case SQLTokenizer.DOUBLE:
						((MutableDouble)value).value += expr.getDouble();
						break;
					case SQLTokenizer.NUMERIC:
					case SQLTokenizer.DECIMAL:
						MutableNumeric newValue = expr.getNumeric();
						if(newValue != null)
							((MutableNumeric)value).add( newValue );
						break;
					case SQLTokenizer.MONEY:
						((Money)value).value += expr.getMoney();
						break;						
					default: throw SmallSQLException.create(Language.UNSUPPORTED_TYPE_SUM, SQLTokenizer.getKeyWord(dataType));
				}
				break;
			case MAX:
				if(value == null){
					if(expr.isNull())
						dataType = expr.getDataType();
					else
						initValue( expr );
				}else if(!expr.isNull()){
					switch(dataType){
						case SQLTokenizer.TINYINT:
						case SQLTokenizer.SMALLINT:
						case SQLTokenizer.INT:
							((MutableInteger)value).value = Math.max( ((MutableInteger)value).value, expr.getInt());
							break;
						case SQLTokenizer.BIGINT:
							((MutableLong)value).value = Math.max( ((MutableLong)value).value, expr.getLong());
							break;
						case SQLTokenizer.REAL:
							((MutableFloat)value).value = Math.max( ((MutableFloat)value).value, expr.getFloat());
							break;
						case SQLTokenizer.FLOAT:
						case SQLTokenizer.DOUBLE:
							((MutableDouble)value).value = Math.max( ((MutableDouble)value).value, expr.getDouble());
							break;
						case SQLTokenizer.CHAR:
						case SQLTokenizer.VARCHAR:
						case SQLTokenizer.LONGVARCHAR:
							String str = expr.getString();
							if(String.CASE_INSENSITIVE_ORDER.compare( (String)value, str ) < 0) //cast needed for Compiler 1.5
								value = str;
							break;
						case SQLTokenizer.NUMERIC:
						case SQLTokenizer.DECIMAL:
							MutableNumeric newValue = expr.getNumeric();
							if(((MutableNumeric)value).compareTo( newValue ) < 0)
								value = newValue;
							break;
						case SQLTokenizer.MONEY:
							((Money)value).value = Math.max( ((Money)value).value, expr.getMoney());
							break;
						case SQLTokenizer.TIMESTAMP:
						case SQLTokenizer.SMALLDATETIME:
						case SQLTokenizer.DATE:
						case SQLTokenizer.TIME:
							((DateTime)value).time = Math.max( ((DateTime)value).time, expr.getLong());
							break;
						case SQLTokenizer.UNIQUEIDENTIFIER:
							// uuid are fixed-len uppercase hex strings and can be correctly 
							// compared with compareTo()
							String uuidStr = expr.getString();
							if (uuidStr.compareTo( (String)value) > 0) value = uuidStr;
							break;
						default:
							String keyword = SQLTokenizer.getKeyWord(dataType);
							throw SmallSQLException.create(Language.UNSUPPORTED_TYPE_MAX, keyword);
					}
				}
				break;
			case MIN:
				if(value == null){
					if(expr.isNull())
						dataType = expr.getDataType();
					else
						initValue( expr );
				}else if(!expr.isNull()){
					switch(dataType){
						case SQLTokenizer.TINYINT:
						case SQLTokenizer.SMALLINT:
						case SQLTokenizer.INT:
							((MutableInteger)value).value = Math.min( ((MutableInteger)value).value, expr.getInt());
							break;
						case SQLTokenizer.BIGINT:
							((MutableLong)value).value = Math.min( ((MutableLong)value).value, expr.getLong());
							break;
						case SQLTokenizer.REAL:
							((MutableFloat)value).value = Math.min( ((MutableFloat)value).value, expr.getFloat());
							break;
						case SQLTokenizer.FLOAT:
						case SQLTokenizer.DOUBLE:
							((MutableDouble)value).value = Math.min( ((MutableDouble)value).value, expr.getDouble());
							break;
						case SQLTokenizer.CHAR:
						case SQLTokenizer.VARCHAR:
						case SQLTokenizer.LONGVARCHAR:
							String str = expr.getString();
							if(String.CASE_INSENSITIVE_ORDER.compare( (String)value, str ) > 0) //cast needed for Compiler 1.5
								value = str;
							break;
						case SQLTokenizer.NUMERIC:
						case SQLTokenizer.DECIMAL:
							MutableNumeric newValue = expr.getNumeric();
							if(((MutableNumeric)value).compareTo( newValue ) > 0)
								value = newValue;
							break;
						case SQLTokenizer.MONEY:
							((Money)value).value = Math.min( ((Money)value).value, expr.getMoney());
							break;
						case SQLTokenizer.TIMESTAMP:
						case SQLTokenizer.SMALLDATETIME:
						case SQLTokenizer.DATE:
						case SQLTokenizer.TIME:
							((DateTime)value).time = Math.min( ((DateTime)value).time, expr.getLong());
							break;
						default: throw new Error(""+dataType);
					}
				}
				break;
			default: throw new Error();
		}
	}

    
    /**
     * Init a summary field with a Mutable 
     * @param expr the expression that produce the values which should be summary
     * @throws Exception
     */
	private void initValue(Expression expr) throws Exception{
		dataType = expr.getDataType();
		switch(dataType){
			case SQLTokenizer.TINYINT:
			case SQLTokenizer.SMALLINT:
			case SQLTokenizer.INT:
				value = new MutableInteger(expr.getInt());
				break;
			case SQLTokenizer.BIGINT:
				value = new MutableLong(expr.getLong());
				break;
			case SQLTokenizer.REAL:
				value = new MutableFloat(expr.getFloat());
				break;
			case SQLTokenizer.FLOAT:
			case SQLTokenizer.DOUBLE:
				value = new MutableDouble(expr.getDouble());
				break;
			case SQLTokenizer.SMALLMONEY:
			case SQLTokenizer.MONEY:
				value = Money.createFromUnscaledValue(expr.getMoney());
				break;
			case SQLTokenizer.NUMERIC:
			case SQLTokenizer.DECIMAL:
				value = new MutableNumeric(expr.getNumeric());
				break;
			case SQLTokenizer.TIMESTAMP:
			case SQLTokenizer.SMALLDATETIME:
			case SQLTokenizer.DATE:
			case SQLTokenizer.TIME:
				value = new DateTime(expr.getLong(), dataType);
				break;
			default: 
				// is used for MAX and MIN
				value = expr.getObject();
		}
	}
/*==============================================================================
methods for PreparedStatement parameters
==============================================================================*/
    private static final Object EMPTY = new Object();
    final boolean isEmpty(){
        return value == EMPTY;
    }

    final void clear(){
        value = EMPTY;
    }
    

	final void set( Object value, int _dataType, int length ) throws SQLException{
		set( value, _dataType );
		this.length = length;
	}
	
	
    /**
     * 
     * @param newValue The new Value.
     * @param newDataType The data type of the new Value (One of the SQLTokenizer const). 
     * If the type is -1 then the data type is verify with many instanceof expressions.
     * @throws SQLException If the newValue is not a instance of a know class. 
     */
    final void set( Object newValue, int newDataType ) throws SQLException{
        this.value      = newValue;
        this.dataType   = newDataType;
        if(dataType < 0){
            if(newValue == null)
                this.dataType = SQLTokenizer.NULL;
            else
            if(newValue instanceof String)
                this.dataType = SQLTokenizer.VARCHAR;
            else
            if(newValue instanceof Byte)
                this.dataType = SQLTokenizer.TINYINT;
            else
            if(newValue instanceof Short)
                this.dataType = SQLTokenizer.SMALLINT;
            else
            if(newValue instanceof Integer)
                this.dataType = SQLTokenizer.INT;
            else
            if(newValue instanceof Long || newValue instanceof Identity)
                this.dataType = SQLTokenizer.BIGINT;
            else
            if(newValue instanceof Float)
                this.dataType = SQLTokenizer.REAL;
            else
            if(newValue instanceof Double)
                this.dataType = SQLTokenizer.DOUBLE;
            else
            if(newValue instanceof Number)
                this.dataType = SQLTokenizer.DECIMAL;
            else
            if(newValue instanceof java.util.Date){
                DateTime dateTime;
            	this.value = dateTime = DateTime.valueOf((java.util.Date)newValue);
				this.dataType = dateTime.getDataType();
            }else
            if(newValue instanceof byte[])
                this.dataType = SQLTokenizer.VARBINARY;
            else
            if(newValue instanceof Boolean)
                this.dataType = SQLTokenizer.BOOLEAN;
            else
            if(newValue instanceof Money)
                this.dataType = SQLTokenizer.MONEY;
            else
                throw SmallSQLException.create(Language.PARAM_CLASS_UNKNOWN, newValue.getClass().getName());
        }
    }
    

    final void set(ExpressionValue val){
    	this.value 		= val.value;
    	this.dataType	= val.dataType;
    	this.length		= val.length;
    }
/*==============================================================================
overriden abstact methods extends from expression
==============================================================================*/


    boolean isNull(){
        return getObject() == null;
    }

    boolean getBoolean() throws Exception{
		return getBoolean( getObject(), dataType );
    }
	
	static boolean getBoolean(Object obj, int dataType) throws Exception{
        if(obj == null) return false;
        switch(dataType){
            case SQLTokenizer.BIT:
            case SQLTokenizer.BOOLEAN:
                return (obj.equals(Boolean.TRUE));
            case SQLTokenizer.TINYINT:
            case SQLTokenizer.SMALLINT:
            case SQLTokenizer.INT:
            case SQLTokenizer.BIGINT:
                return ((Number)obj).intValue() != 0;
            case SQLTokenizer.REAL:
            case SQLTokenizer.DOUBLE:
            case SQLTokenizer.MONEY:
                return ((Number)obj).doubleValue() != 0;
            default: return Utils.string2boolean( obj.toString() );
        }
    }

    int getInt() throws Exception{
		return getInt( getObject(), dataType );
    }
	
	static int getInt(Object obj, int dataType) throws Exception{
        if(obj == null) return 0;
        switch(dataType){
            case SQLTokenizer.BIT:
            case SQLTokenizer.BOOLEAN:
                return (obj == Boolean.TRUE) ? 1 : 0;
            case SQLTokenizer.TINYINT:
            case SQLTokenizer.SMALLINT:
            case SQLTokenizer.INT:
            case SQLTokenizer.BIGINT:
            case SQLTokenizer.REAL:
            case SQLTokenizer.FLOAT:
            case SQLTokenizer.DOUBLE:
            case SQLTokenizer.MONEY:
            case SQLTokenizer.DECIMAL:
            case SQLTokenizer.NUMERIC:
                return ((Number)obj).intValue();
			case SQLTokenizer.TIMESTAMP:
			case SQLTokenizer.TIME:
			case SQLTokenizer.DATE:
			case SQLTokenizer.SMALLDATETIME:
				return (int)((DateTime)obj).getTimeMillis();
            default:
				String str = obj.toString().trim();
				try{
					return Integer.parseInt( str );
				}catch(Throwable th){/* A NumberFormatException can occur if it a floating point number */}
				return (int)Double.parseDouble( str );
        }
    }

    long getLong() throws Exception{
    	return getLong( getObject(), dataType);
    }
    
	static long getLong(Object obj, int dataType) throws Exception{
       if(obj == null) return 0;
        switch(dataType){
            case SQLTokenizer.BIT:
            case SQLTokenizer.BOOLEAN:
                return (obj == Boolean.TRUE) ? 1 : 0;
            case SQLTokenizer.TINYINT:
            case SQLTokenizer.SMALLINT:
            case SQLTokenizer.INT:
            case SQLTokenizer.BIGINT:
            case SQLTokenizer.DOUBLE:
            case SQLTokenizer.MONEY:
                return ((Number)obj).longValue();
			case SQLTokenizer.TIMESTAMP:
			case SQLTokenizer.TIME:
			case SQLTokenizer.DATE:
			case SQLTokenizer.SMALLDATETIME:
				return ((DateTime)obj).getTimeMillis();
            default: 
            	String str = obj.toString();
            	if(str.indexOf('-') > 0 || str.indexOf(':') > 0)
            		return DateTime.parse(str);
				try{
					return Long.parseLong( str );
				}catch(NumberFormatException e){
					return (long)Double.parseDouble( str );
				}
        }
    }

    float getFloat() throws Exception{
		return getFloat( getObject(), dataType);
    }
	
	static float getFloat(Object obj, int dataType) throws Exception{
        if(obj == null) return 0;
        switch(dataType){
            case SQLTokenizer.BIT:
                return (obj.equals(Boolean.TRUE)) ? 1 : 0;
            case SQLTokenizer.INT:
            case SQLTokenizer.BIGINT:
            case SQLTokenizer.DOUBLE:
			case SQLTokenizer.FLOAT:
            case SQLTokenizer.REAL:
            case SQLTokenizer.MONEY:
                return ((Number)obj).floatValue();
			case SQLTokenizer.TIMESTAMP:
			case SQLTokenizer.TIME:
			case SQLTokenizer.DATE:
			case SQLTokenizer.SMALLDATETIME:
				return ((DateTime)obj).getTimeMillis();
            default: return Float.parseFloat( obj.toString() );
        }
    }

    double getDouble() throws Exception{
		return getDouble( getObject(), dataType);
    }
	
	static double getDouble(Object obj, int dataType) throws Exception{
        if(obj == null) return 0;
        switch(dataType){
            case SQLTokenizer.BIT:
                return (obj.equals(Boolean.TRUE)) ? 1 : 0;
            case SQLTokenizer.INT:
            case SQLTokenizer.BIGINT:
            case SQLTokenizer.DOUBLE:
            case SQLTokenizer.MONEY:
                return ((Number)obj).doubleValue();
			case SQLTokenizer.TIMESTAMP:
			case SQLTokenizer.TIME:
			case SQLTokenizer.DATE:
			case SQLTokenizer.SMALLDATETIME:
				return ((DateTime)obj).getTimeMillis();
            default: return Double.parseDouble( obj.toString() );
        }
    }


    long getMoney() throws Exception{
		return getMoney( getObject(), dataType );
    }


    static long getMoney(Object obj, int dataType) throws Exception{
        if(obj == null) return 0;
        switch(dataType){
            case SQLTokenizer.BIT:
                return (obj == Boolean.TRUE) ? 10000 : 0;
            case SQLTokenizer.TINYINT:
            case SQLTokenizer.SMALLINT:
            case SQLTokenizer.INT:
            case SQLTokenizer.BIGINT:
                return ((Number)obj).longValue() * 10000;
            case SQLTokenizer.REAL:
            case SQLTokenizer.FLOAT:
            case SQLTokenizer.DOUBLE:
                return Utils.doubleToMoney(((Number)obj).doubleValue());
            case SQLTokenizer.MONEY:
            case SQLTokenizer.SMALLMONEY:
            	return ((Money)obj).value;
            default: return Money.parseMoney( obj.toString() );
        }
	}


    MutableNumeric getNumeric(){
		return getNumeric( getObject(), dataType );
    }


    static MutableNumeric getNumeric(Object obj, int dataType){
        if(obj == null) return null;
        switch(dataType){
            case SQLTokenizer.BIT:
                return new MutableNumeric( (obj == Boolean.TRUE) ? 1 : 0);
            case SQLTokenizer.INT:
                return new MutableNumeric( ((Number)obj).intValue() );
            case SQLTokenizer.BIGINT:
                return new MutableNumeric( ((Number)obj).longValue() );
            case SQLTokenizer.REAL:
                float fValue = ((Number)obj).floatValue();
                if(Float.isInfinite(fValue) || Float.isNaN(fValue))
                    return null;
                return new MutableNumeric( fValue );
            case SQLTokenizer.FLOAT:
            case SQLTokenizer.DOUBLE:
                double dValue = ((Number)obj).doubleValue();
                if(Double.isInfinite(dValue) || Double.isNaN(dValue))
                    return null;
                return new MutableNumeric( dValue );
            case SQLTokenizer.MONEY:
            case SQLTokenizer.SMALLMONEY:
            	return new MutableNumeric( ((Money)obj).value, 4 );
            case SQLTokenizer.DECIMAL:
            case SQLTokenizer.NUMERIC:
				if(obj instanceof MutableNumeric)
					return (MutableNumeric)obj;
				return new MutableNumeric( (BigDecimal)obj );
            default: return new MutableNumeric( obj.toString() );
        }
	}


    Object getObject(){
        if(isEmpty()){
            return null;
        }
        return value;
    }

    String getString(){
        Object obj = getObject();
        if(obj == null) return null;
        if(dataType == SQLTokenizer.BIT){
            return (obj == Boolean.TRUE) ? "1" : "0";
        }
        return obj.toString();
    }

    byte[] getBytes() throws Exception{
    	return getBytes( getObject(), dataType);
    }
    
    
	static byte[] getBytes(Object obj, int dataType) throws Exception{
		if(obj == null) return null;
		switch(dataType){
			case SQLTokenizer.BINARY:
			case SQLTokenizer.VARBINARY:
            case SQLTokenizer.LONGVARBINARY:
				return (byte[])obj;
			case SQLTokenizer.VARCHAR:
			case SQLTokenizer.CHAR:
			case SQLTokenizer.NVARCHAR:
			case SQLTokenizer.NCHAR:
				return ((String)obj).getBytes();
			case SQLTokenizer.UNIQUEIDENTIFIER:
				return Utils.unique2bytes((String)obj);
            case SQLTokenizer.INT:
                return Utils.int2bytes( ((Number)obj).intValue() );
            case SQLTokenizer.DOUBLE:
                return Utils.double2bytes( ((Number)obj).doubleValue() );
            case SQLTokenizer.REAL:
                return Utils.float2bytes( ((Number)obj).floatValue() );
			default: throw createUnsupportedConversion(dataType, obj, SQLTokenizer.VARBINARY);
		}
	}
    
    

    final int getDataType(){
        return dataType;
    }

	/*=======================================================================
	 
		Methods for ResultSetMetaData
	 
	=======================================================================*/

	String getTableName(){
		return null;
	}

	final int getPrecision(){
		switch(dataType){
			case SQLTokenizer.VARCHAR:
			case SQLTokenizer.CHAR:
				return ((String)value).length();
			case SQLTokenizer.VARBINARY:
			case SQLTokenizer.BINARY:
				return ((byte[])value).length;
			default: 
				return super.getPrecision();
		}
	}
	
	
	int getScale(){
		switch(dataType){
			case SQLTokenizer.DECIMAL:
			case SQLTokenizer.NUMERIC:
				MutableNumeric obj = getNumeric();
				return (obj == null) ? 0: obj.getScale();
			default:
				return getScale(dataType);
		}
	}
	

	static SQLException createUnsupportedConversion( int fromDataType, Object obj, int toDataType ){
		Object[] params = {
			SQLTokenizer.getKeyWord(fromDataType),
			obj,
			SQLTokenizer.getKeyWord(toDataType)
		};
		
        return SmallSQLException.create(Language.UNSUPPORTED_CONVERSION, params);
    }


}