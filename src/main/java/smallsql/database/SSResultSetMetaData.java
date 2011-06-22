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
 * SSResultSetMetaData.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.database;

import java.sql.*;

import smallsql.database.language.Language;


public class SSResultSetMetaData implements ResultSetMetaData {

    Expressions columns;

    public int getColumnCount() throws SQLException {
        return columns.size();
    }
    
    
    public boolean isAutoIncrement(int column) throws SQLException {
        return getColumnExpression( column ).isAutoIncrement();
    }
    
    
    public boolean isCaseSensitive(int column) throws SQLException {
        return getColumnExpression( column ).isCaseSensitive();
    }
    
    
    public boolean isSearchable(int column) throws SQLException {
    	int type = getColumnExpression( column ).getType();
        return type == Expression.NAME || type == Expression.FUNCTION;
    }
    
    
    public boolean isCurrency(int column) throws SQLException {
        switch(getColumnExpression( column ).getDataType()){
            case SQLTokenizer.MONEY:
            case SQLTokenizer.SMALLMONEY:
                return true;
        }
        return false;
    }
    
    
    public int isNullable(int column) throws SQLException {
        return getColumnExpression( column ).isNullable() ? columnNullable : columnNoNulls;
    }
    
    
    public boolean isSigned(int column) throws SQLException {
		return isSignedDataType(getColumnExpression( column ).getDataType());
    }
    
    
	static boolean isSignedDataType(int dataType) {
		switch(dataType){
			case SQLTokenizer.SMALLINT:
			case SQLTokenizer.INT:
			case SQLTokenizer.BIGINT:
			case SQLTokenizer.SMALLMONEY:
			case SQLTokenizer.MONEY:
			case SQLTokenizer.DECIMAL:
			case SQLTokenizer.NUMERIC:
			case SQLTokenizer.REAL:
			case SQLTokenizer.FLOAT:
			case SQLTokenizer.DOUBLE:
				return true;
		}
		return false;
	}
	
    
	static boolean isNumberDataType(int dataType) {
		return isSignedDataType(dataType) || dataType == SQLTokenizer.TINYINT;
	}
	
    
	static boolean isBinaryDataType(int dataType) {
		switch(dataType){
			case SQLTokenizer.BINARY:
			case SQLTokenizer.VARBINARY:
			case SQLTokenizer.LONGVARBINARY:
			case SQLTokenizer.BLOB:
				return true;
		}
		return false;
	}
	
	
	static int getDisplaySize(int dataType, int precision, int scale){
		switch(dataType){
			case SQLTokenizer.BIT:
				return 1; // 1 and 0
			case SQLTokenizer.BOOLEAN:
				return 5; //true and false
			case SQLTokenizer.TINYINT:
				return 3;
			case SQLTokenizer.SMALLINT:
				return 6;
			case SQLTokenizer.INT:
				return 10;
			case SQLTokenizer.BIGINT:
            case SQLTokenizer.MONEY:
				return 19;
            case SQLTokenizer.REAL:
                return 13;
			case SQLTokenizer.FLOAT:
			case SQLTokenizer.DOUBLE:
				return 17;
			case SQLTokenizer.LONGVARCHAR:
            case SQLTokenizer.LONGNVARCHAR:
			case SQLTokenizer.LONGVARBINARY:
            case SQLTokenizer.JAVA_OBJECT:
            case SQLTokenizer.BLOB:
            case SQLTokenizer.CLOB:
            case SQLTokenizer.NCLOB:
				return Integer.MAX_VALUE;
			case SQLTokenizer.NUMERIC:
				return precision + (scale>0 ? 2 : 1);
			case SQLTokenizer.VARBINARY:
			case SQLTokenizer.BINARY:
				return 2 + precision*2;
            case SQLTokenizer.SMALLDATETIME:
                return 21;
			default:
				return precision;
		}
	}
	
    
	static int getDataTypePrecision(int dataType, int defaultValue){
		switch(dataType){
			case SQLTokenizer.NULL:
				return 0;
			case SQLTokenizer.BIT:
			case SQLTokenizer.BOOLEAN:
				return 1;
			case SQLTokenizer.TINYINT:
				return 3;
			case SQLTokenizer.SMALLINT:
				return 5;
			case SQLTokenizer.INT:
			case SQLTokenizer.SMALLMONEY:
				return 10;
			case SQLTokenizer.BIGINT:
			case SQLTokenizer.MONEY:
				return 19;
			case SQLTokenizer.REAL:
				return 7;
			case SQLTokenizer.FLOAT:
			case SQLTokenizer.DOUBLE:
				return 15;
            case SQLTokenizer.CHAR:
            case SQLTokenizer.NCHAR:
			case SQLTokenizer.VARCHAR:
            case SQLTokenizer.NVARCHAR:
            case SQLTokenizer.BINARY:
			case SQLTokenizer.VARBINARY:
				if(defaultValue == -1)
					return 0xFFFF;
                return defaultValue;
			case SQLTokenizer.NUMERIC:
			case SQLTokenizer.DECIMAL:
                if(defaultValue == -1)
                    return 38;
                return defaultValue;
			case SQLTokenizer.TIMESTAMP:
				return 23;
			case SQLTokenizer.TIME:
				return 8;
			case SQLTokenizer.DATE:
				return 10;
			case SQLTokenizer.SMALLDATETIME:
				return 16;
			case SQLTokenizer.UNIQUEIDENTIFIER:
				return 36;
			case SQLTokenizer.LONGVARCHAR:
            case SQLTokenizer.LONGNVARCHAR:
			case SQLTokenizer.LONGVARBINARY:
				return Integer.MAX_VALUE;
		}
		if(defaultValue == -1)
			throw new Error("Precision:"+SQLTokenizer.getKeyWord(dataType));
		return defaultValue;
	}
	
	
    public int getColumnDisplaySize(int column) throws SQLException {
        return getColumnExpression( column ).getDisplaySize();
    }
    public String getColumnLabel(int column) throws SQLException {
        return getColumnExpression( column ).getAlias();
    }
    public String getColumnName(int column) throws SQLException {
        return getColumnExpression( column ).getAlias();
    }
    public String getSchemaName(int column) throws SQLException {
        return null;
    }
    public int getPrecision(int column) throws SQLException {
        return getColumnExpression( column ).getPrecision();
    }
    public int getScale(int column) throws SQLException {
        return getColumnExpression( column ).getScale();
    }
    public String getTableName(int column) throws SQLException {
        return getColumnExpression( column ).getTableName();
    }
    public String getCatalogName(int column) throws SQLException {
        return null;
    }
    public int getColumnType(int column) throws SQLException {
        return SQLTokenizer.getSQLDataType(getColumnExpression( column ).getDataType() );
    }
    public String getColumnTypeName(int column) throws SQLException {
        return SQLTokenizer.getKeyWord( getColumnExpression( column ).getDataType() );
    }
    public boolean isReadOnly(int column) throws SQLException {
        return !getColumnExpression( column ).isDefinitelyWritable();
    }
    public boolean isWritable(int column) throws SQLException {
        return getColumnExpression( column ).isDefinitelyWritable();
    }
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return getColumnExpression( column ).isDefinitelyWritable();
    }
    public String getColumnClassName(int column) throws SQLException {
        switch(getColumnType(column)){
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
                    return "java.lang.Integer";
            case Types.BIT:
            case Types.BOOLEAN:
                    return "java.lang.Boolean";
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                    return "[B";
            case Types.BLOB:
                    return "java.sql.Blob";
            case Types.BIGINT:
                    return "java.lang.Long";
            case Types.DECIMAL:
            case Types.NUMERIC:
                    return "java.math.BigDecimal";
            case Types.REAL:
                    return "java.lang.Float";
            case Types.FLOAT:
            case Types.DOUBLE:
                    return "java.lang.Double";
            case Types.DATE:
                    return "java.sql.Date";
            case Types.TIME:
                    return "java.sql.Time";
            case Types.TIMESTAMP:
                    return "java.sql.Timestamp";
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case -11: //uniqueidentifier
                    return "java.lang.String";
            case Types.CLOB:
                    return "java.sql.Clob";
            default: return "java.lang.Object";
        }
    }

/*========================================================

private methods

=========================================================*/

	final int getColumnIdx( int column ) throws SQLException{
		if(column < 1 || column > columns.size())
			throw SmallSQLException.create( Language.COL_IDX_OUT_RANGE, String.valueOf(column));
		return column-1;
	}

    final Expression getColumnExpression( int column ) throws SQLException{
        return columns.get( getColumnIdx( column ) );
    }


}