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
 * SSPreparedStatement.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.database;

import java.sql.*;
import java.math.*;
import java.io.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.net.URL;


class SSPreparedStatement extends SSStatement implements PreparedStatement {

	private ArrayList batches;
    private final int top; // value of an optional top expression
	
    SSPreparedStatement( SSConnection con, String sql ) throws SQLException {
        this( con, sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY );
    }

    SSPreparedStatement( SSConnection con, String sql, int rsType, int rsConcurrency ) throws SQLException {
        super( con, rsType, rsConcurrency );
        con.log.println(sql);
        SQLParser parser = new SQLParser();
        cmd = parser.parse( con, sql );
        top = cmd.getMaxRows();
    }

    public ResultSet executeQuery() throws SQLException {
		executeImp();
        return cmd.getQueryResult();
    }
    
    public int executeUpdate() throws SQLException {
		executeImp();
		return cmd.getUpdateCount();
    }
    
	final private void executeImp() throws SQLException {
        checkStatement();
		cmd.verifyParams();
        if(getMaxRows() != 0 && (top == -1 || top > getMaxRows()))
            cmd.setMaxRows(getMaxRows());
		cmd.execute( con, this);
	}
    
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        checkStatement();
        cmd.setParamValue( parameterIndex, null, SQLTokenizer.NULL);
    }
    
    
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        checkStatement();
        cmd.setParamValue( parameterIndex, x ? Boolean.TRUE : Boolean.FALSE, SQLTokenizer.BOOLEAN);
    }
    
    
    public void setByte(int parameterIndex, byte x) throws SQLException {
        checkStatement();
        cmd.setParamValue( parameterIndex, new Integer(x), SQLTokenizer.TINYINT);
    }
    
    
    public void setShort(int parameterIndex, short x) throws SQLException {
        checkStatement();
        cmd.setParamValue( parameterIndex, new Integer(x), SQLTokenizer.SMALLINT);
    }
    
    
    public void setInt(int parameterIndex, int x) throws SQLException {
        checkStatement();
        cmd.setParamValue( parameterIndex, new Integer(x), SQLTokenizer.INT);
    }
    
    
    public void setLong(int parameterIndex, long x) throws SQLException {
        checkStatement();
        cmd.setParamValue( parameterIndex, new Long(x), SQLTokenizer.BIGINT);
    }
    
    
    public void setFloat(int parameterIndex, float x) throws SQLException {
        checkStatement();
        cmd.setParamValue( parameterIndex, new Float(x), SQLTokenizer.REAL);
    }
    
    
    public void setDouble(int parameterIndex, double x) throws SQLException {
        checkStatement();
        cmd.setParamValue( parameterIndex, new Double(x), SQLTokenizer.DOUBLE);
    }
    
    
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        checkStatement();
        cmd.setParamValue( parameterIndex, x, SQLTokenizer.DECIMAL);
    }
    
    
    public void setString(int parameterIndex, String x) throws SQLException {
        checkStatement();
        cmd.setParamValue( parameterIndex, x, SQLTokenizer.VARCHAR);
    }
    
    
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        checkStatement();
        cmd.setParamValue( parameterIndex, x, SQLTokenizer.BINARY);
    }
    
    
    public void setDate(int parameterIndex, Date x) throws SQLException {
        checkStatement();
        cmd.setParamValue( parameterIndex, DateTime.valueOf(x), SQLTokenizer.DATE);
    }
    
    
    public void setTime(int parameterIndex, Time x) throws SQLException {
        checkStatement();
        cmd.setParamValue( parameterIndex, DateTime.valueOf(x), SQLTokenizer.TIME);
    }
    
    
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        checkStatement();
        cmd.setParamValue( parameterIndex, DateTime.valueOf(x), SQLTokenizer.TIMESTAMP);
    }
    
    
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        checkStatement();
		cmd.setParamValue( parameterIndex, x, SQLTokenizer.LONGVARCHAR, length);
    }
    
    
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        checkStatement();
        /**@todo: Implement this java.sql.PreparedStatement method*/
        throw new java.lang.UnsupportedOperationException("Method setUnicodeStream() not yet implemented.");
    }
    
    
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        checkStatement();
		cmd.setParamValue( parameterIndex, x, SQLTokenizer.LONGVARBINARY, length);
    }
    
    
    public void clearParameters() throws SQLException {
        checkStatement();
        cmd.clearParams();
    }
    
    
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scale) throws SQLException {
        checkStatement();
    	//FIXME Scale to consider 
		cmd.setParamValue( parameterIndex, x, -1);
    }
    
    
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        checkStatement();
        cmd.setParamValue( parameterIndex, x, -1);
    }
    
    
    public void setObject(int parameterIndex, Object x) throws SQLException {
        checkStatement();
        cmd.setParamValue( parameterIndex, x, -1);
    }
    
    
    public boolean execute() throws SQLException {
		executeImp();
        return cmd.getResultSet() != null;
    }
    
    
    public void addBatch() throws SQLException {
        checkStatement();
    	try{
	    	final Expressions params = cmd.params;
	    	final int size = params.size();
			ExpressionValue[] values = new ExpressionValue[size];
	    	for(int i=0; i<size; i++){
	    		values[i] = (ExpressionValue)params.get(i).clone();
	    	}
	    	if(batches == null) batches = new ArrayList();
	    	batches.add(values);
    	}catch(Exception e){
    		throw SmallSQLException.createFromException(e);
    	}
    }
    
    
	public void clearBatch() throws SQLException {
        checkStatement();
		if(batches != null) batches.clear();
	}
	
	
    public int[] executeBatch() throws BatchUpdateException {
		if(batches == null || batches.size() == 0) return new int[0];
		int[] result = new int[batches.size()];
		BatchUpdateException failed = null;
		for(int b=0; b<batches.size(); b++){
			try{
                checkStatement();
				ExpressionValue[] values = (ExpressionValue[])batches.get(b);
				for(int i=0; i<values.length; i++){
					((ExpressionValue)cmd.params.get(i)).set( values[i] );
				}
				result[b] = executeUpdate();
			} catch (SQLException ex) {
				result[b] = EXECUTE_FAILED;
				if(failed == null){
					failed = new BatchUpdateException(ex.getMessage(), ex.getSQLState(), ex.getErrorCode(), result);
					failed.initCause(ex);
				}
				failed.setNextException(ex);
			}
		}
    	batches.clear();
    	if(failed != null)
    		throw failed;
    	return result;
	}
	
	
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        checkStatement();
        /**@todo: Implement this java.sql.PreparedStatement method*/
        throw new java.lang.UnsupportedOperationException("Method setCharacterStream() not yet implemented.");
    }
    
    
    public void setRef(int i, Ref x) throws SQLException {
        checkStatement();
        /**@todo: Implement this java.sql.PreparedStatement method*/
        throw new java.lang.UnsupportedOperationException("Method setRef() not yet implemented.");
    }
    public void setBlob(int i, Blob x) throws SQLException {
        checkStatement();
        /**@todo: Implement this java.sql.PreparedStatement method*/
        throw new java.lang.UnsupportedOperationException("Method setBlob() not yet implemented.");
    }
    public void setClob(int i, Clob x) throws SQLException {
        checkStatement();
        /**@todo: Implement this java.sql.PreparedStatement method*/
        throw new java.lang.UnsupportedOperationException("Method setClob() not yet implemented.");
    }
    public void setArray(int i, Array x) throws SQLException {
        checkStatement();
        /**@todo: Implement this java.sql.PreparedStatement method*/
        throw new java.lang.UnsupportedOperationException("Method setArray() not yet implemented.");
    }
	
	
    public ResultSetMetaData getMetaData() throws SQLException {
        checkStatement();
		if(cmd instanceof CommandSelect){
			try{
				((CommandSelect)cmd).compile(con);
				SSResultSetMetaData metaData = new SSResultSetMetaData();
				metaData.columns = cmd.columnExpressions;
				return metaData;
			}catch(Exception e){
				throw SmallSQLException.createFromException(e);
			}
		}
		return null;
    }
	
	
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        checkStatement();
        /**@todo: Implement this java.sql.PreparedStatement method*/
        throw new java.lang.UnsupportedOperationException("Method setDate() not yet implemented.");
    }
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        checkStatement();
        /**@todo: Implement this java.sql.PreparedStatement method*/
        throw new java.lang.UnsupportedOperationException("Method setTime() not yet implemented.");
    }
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        checkStatement();
        /**@todo: Implement this java.sql.PreparedStatement method*/
        throw new java.lang.UnsupportedOperationException("Method setTimestamp() not yet implemented.");
    }
    public void setNull(int paramIndex, int sqlType, String typeName) throws SQLException {
        checkStatement();
        /**@todo: Implement this java.sql.PreparedStatement method*/
        throw new java.lang.UnsupportedOperationException("Method setNull() not yet implemented.");
    }
    public void setURL(int parameterIndex, URL x) throws SQLException {
        checkStatement();
        /**@todo: Implement this java.sql.PreparedStatement method*/
        throw new java.lang.UnsupportedOperationException("Method setURL() not yet implemented.");
    }
    public ParameterMetaData getParameterMetaData() throws SQLException {
        checkStatement();
       /**@todo: Implement this java.sql.PreparedStatement method*/
        throw new java.lang.UnsupportedOperationException("Method getParameterMetaData() not yet implemented.");
    }
}