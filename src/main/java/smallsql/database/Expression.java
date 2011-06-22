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
 * Expression.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.database;

import java.sql.SQLException;


abstract class Expression implements Cloneable{
	
	static final Expression NULL = new ExpressionValue( null, SQLTokenizer.NULL );

	final private int type;
	private String name; // the name of the original column in the table
	private String alias;
	
	/**
	 * A list of parameters. It is used for ExpressionFunction and ExpressionAritmethik.
	 * Do not modify this variable from extern directly because there are other references.
	 * Use the methods setParams() and setParamAt()
	 * @see #setParams
	 * @see setParamAt
	 */
	private Expression[] params;
	
	Expression(int type){
		this.type = type;
	}
	
	protected Object clone() throws CloneNotSupportedException{
		return super.clone();
	}
	
	final String getName(){ 
		return name; 
	}

	final void setName(String name){ 
		this.alias = this.name = name; 
	}

	final String getAlias(){ 
		return alias; 
	}

	final void setAlias(String alias){ 
		this.alias = alias; 
	}

    void setParams( Expression[] params ){
        this.params = params;
    }
    
    /**
     * Replace the idx parameter. You need to use this method to modify the <code>params</code> 
     * array because there there can be other references to the <code>params</code>. 
     */
    void setParamAt( Expression param, int idx){
    	params[idx] = param;
    }

    final Expression[] getParams(){ return params; }
    
    /**
     * Optimize the expression after a command was compiled. 
     * This can be constant expressions that are evaluate once.
     * @throws SQLException 
     */
    void optimize() throws SQLException{
        if(params != null){
            for(int p=0; p<params.length; p++){
                params[p].optimize();
            }
        }
    }

	/**
	 * Is used in GroupResult.
	 */
	public boolean equals(Object expr){
		if(!(expr instanceof Expression)) return false;
		if( ((Expression)expr).type == type){
			
			Expression[] p1 = ((Expression)expr).params;
			Expression[] p2 = params;
			if(p1 != null && p2 != null){
				if(p1 == null) return false;
				for(int i=0; i<p1.length; i++){
					if(!p2[i].equals(p1[i])) return false;
				}
			}
			String name1 = ((Expression)expr).name;
			String name2 = name;
			if(name1 == name2) return true;
			if(name1 == null) return false;
			if(name1.equalsIgnoreCase(name2)) return true;
		}
		return false;
	}

    
    abstract boolean isNull() throws Exception;

    abstract boolean getBoolean() throws Exception;

    abstract int getInt() throws Exception;

    abstract long getLong() throws Exception;

    abstract float getFloat() throws Exception;

    abstract double getDouble() throws Exception;

    abstract long getMoney() throws Exception;

    abstract MutableNumeric getNumeric() throws Exception;

    abstract Object getObject() throws Exception;

	final Object getApiObject() throws Exception{
		Object obj = getObject();
		if(obj instanceof Mutable){
			return ((Mutable)obj).getImmutableObject();
		}
		return obj;
	}

    abstract String getString() throws Exception;

    abstract byte[] getBytes() throws Exception;

    abstract int getDataType();

    final int getType(){return type;}

	/*=======================================================================
	 
		Methods for ResultSetMetaData
	 
	=======================================================================*/

	String getTableName(){
		return null;
	}
	

	int getPrecision(){
		return SSResultSetMetaData.getDataTypePrecision( getDataType(), -1 );
	}
	
	
	
	int getScale(){
		return getScale(getDataType());
	}
	
	
	final static int getScale(int dataType){
		switch(dataType){
			case SQLTokenizer.MONEY:
			case SQLTokenizer.SMALLMONEY:
				return 4;
			case SQLTokenizer.TIMESTAMP:
				return 9; //nano seconds
			case SQLTokenizer.NUMERIC:
			case SQLTokenizer.DECIMAL:
				return 38;
			default: return 0;
		}
	}


	int getDisplaySize(){
		return SSResultSetMetaData.getDisplaySize(getDataType(), getPrecision(), getScale());
	}

	boolean isDefinitelyWritable(){
		return false;
	}
	
	boolean isAutoIncrement(){
		return false;
	}
	
	boolean isCaseSensitive(){
		return false; 
	}

	boolean isNullable(){
		return true; 
	}


    static final int VALUE      = 1;
    static final int NAME       = 2;
    static final int FUNCTION   = 3;
	static final int GROUP_BY   = 11;
	static final int COUNT	    = 12;
	static final int SUM	    = 13;
	static final int FIRST		= 14;
	static final int LAST		= 15;
	static final int MIN		= 16;
	static final int MAX		= 17;
	static final int GROUP_BEGIN= GROUP_BY;

}