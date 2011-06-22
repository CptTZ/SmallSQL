/* =============================================================
 * SmallSQL : a free Java DBMS library for the Java(tm) platform
 * =============================================================
 *
 * (C) Copyright 2004-2009, by Volker Berlin.
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
 * SSResultSet.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.database;

import java.sql.*;
import java.math.*;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.util.Map;
import java.util.Calendar;
import java.net.URL;
import smallsql.database.language.Language;

public class SSResultSet implements ResultSet {

    SSResultSetMetaData metaData = new SSResultSetMetaData();
    private CommandSelect cmd;
    private boolean wasNull;
    SSStatement st;
    private boolean isUpdatable;
    private boolean isInsertRow;
    private ExpressionValue[] values;
    private int fetchDirection;
    private int fetchSize;

    SSResultSet( SSStatement st, CommandSelect cmd ){
        this.st = st;
        metaData.columns = cmd.columnExpressions;
        this.cmd = cmd;
		isUpdatable = st != null && st.rsConcurrency == CONCUR_UPDATABLE && !cmd.isGroupResult();
    }

/*==============================================================================

    Public Interface

==============================================================================*/

    public void close(){
    	st.con.log.println("ResultSet.close");
        cmd = null;
    }
    
    
    public boolean wasNull(){
        return wasNull;
    }
    
    
    public String getString(int columnIndex) throws SQLException {
        try{
            Object obj = getObject(columnIndex);
            
            if(obj instanceof String || obj == null){
                return (String)obj;
            }
            if(obj instanceof byte[]){
                // The Display Value of a binary Value is different as the default in SQL 
                return "0x" + Utils.bytes2hex( (byte[])obj );
            }
            // all other values
            return getValue(columnIndex).getString();
        }catch(Exception e){
            throw SmallSQLException.createFromException( e );
        }
    }
    public boolean getBoolean(int columnIndex) throws SQLException {
        try{
            Expression expr = getValue(columnIndex);
            wasNull = expr.isNull();
            return expr.getBoolean();
        }catch(Exception e){
            throw SmallSQLException.createFromException( e );
        }
    }
    public byte getByte(int columnIndex) throws SQLException {
        return (byte)getInt( columnIndex );
    }
    public short getShort(int columnIndex) throws SQLException {
        return (short)getInt( columnIndex );
    }
    public int getInt(int columnIndex) throws SQLException {
        try{
            Expression expr = getValue(columnIndex);
            wasNull = expr.isNull();
            return expr.getInt();
        }catch(Exception e){
            throw SmallSQLException.createFromException( e );
        }
    }
    public long getLong(int columnIndex) throws SQLException {
        try{
            Expression expr = getValue(columnIndex);
            wasNull = expr.isNull();
            return expr.getLong();
        }catch(Exception e){
            throw SmallSQLException.createFromException( e );
        }
    }
    public float getFloat(int columnIndex) throws SQLException {
        try{
            Expression expr = getValue(columnIndex);
            wasNull = expr.isNull();
            return expr.getFloat();
        }catch(Exception e){
            throw SmallSQLException.createFromException( e );
        }
    }
    public double getDouble(int columnIndex) throws SQLException {
        try{
            Expression expr = getValue(columnIndex);
            wasNull = expr.isNull();
            return expr.getDouble();
        }catch(Exception e){
            throw SmallSQLException.createFromException( e );
        }
    }
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        try{
            MutableNumeric obj = getValue(columnIndex).getNumeric();
            wasNull = obj == null;
            if(wasNull) return null;
            return obj.toBigDecimal(scale);
        }catch(Exception e){
            throw SmallSQLException.createFromException( e );
        }
    }
    public byte[] getBytes(int columnIndex) throws SQLException {
        try{
            byte[] obj = getValue(columnIndex).getBytes();
            wasNull = obj == null;
            return obj;
        }catch(Exception e){
            throw SmallSQLException.createFromException( e );
        }
    }
    public Date getDate(int columnIndex) throws SQLException {
        try{
			Expression expr = getValue(columnIndex);
            wasNull = expr.isNull();
			if(wasNull) return null;
			return DateTime.getDate( expr.getLong() );
        }catch(Exception e){
            throw SmallSQLException.createFromException( e );
        }
    }
    
    
    public Time getTime(int columnIndex) throws SQLException {
        try{
			Expression expr = getValue(columnIndex);
            wasNull = expr.isNull();
			if(wasNull) return null;
			return DateTime.getTime( expr.getLong() );
        }catch(Exception e){
            throw SmallSQLException.createFromException( e );
        }
    }
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        try{
			Expression expr = getValue(columnIndex);
            wasNull = expr.isNull();
			if(wasNull) return null;
			return DateTime.getTimestamp( expr.getLong() );
        }catch(Exception e){
            throw SmallSQLException.createFromException( e );
        }
    }
    
    
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        /**@todo: Implement this java.sql.ResultSet.getAsciiStream method*/
        throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION, "getAsciiStream");
    }
    
    
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        /**@todo: Implement this java.sql.ResultSet.getUnicodeStream method*/
        throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION, "getUnicodeStream");
    }
    
    
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return new ByteArrayInputStream(getBytes(columnIndex));
    }
    
    
    public String getString(String columnName) throws SQLException {
        return getString( findColumn( columnName ) );
    }
    public boolean getBoolean(String columnName) throws SQLException {
        return getBoolean( findColumn( columnName ) );
    }
    public byte getByte(String columnName) throws SQLException {
        return getByte( findColumn( columnName ) );
    }
    public short getShort(String columnName) throws SQLException {
        return getShort( findColumn( columnName ) );
    }
    public int getInt(String columnName) throws SQLException {
        return getInt( findColumn( columnName ) );
    }
    public long getLong(String columnName) throws SQLException {
        return getLong( findColumn( columnName ) );
    }
    public float getFloat(String columnName) throws SQLException {
        return getFloat( findColumn( columnName ) );
    }
    public double getDouble(String columnName) throws SQLException {
        return getDouble( findColumn( columnName ) );
    }
    public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException {
        return getBigDecimal( findColumn( columnName ), scale );
    }
    public byte[] getBytes(String columnName) throws SQLException {
        return getBytes( findColumn( columnName ) );
    }
    public Date getDate(String columnName) throws SQLException {
        return getDate( findColumn( columnName ) );
    }
    public Time getTime(String columnName) throws SQLException {
        return getTime( findColumn( columnName ) );
    }
    public Timestamp getTimestamp(String columnName) throws SQLException {
        return getTimestamp( findColumn( columnName ) );
    }
    public InputStream getAsciiStream(String columnName) throws SQLException {
        return getAsciiStream( findColumn( columnName ) );
    }
    public InputStream getUnicodeStream(String columnName) throws SQLException {
        return getUnicodeStream( findColumn( columnName ) );
    }
    public InputStream getBinaryStream(String columnName) throws SQLException {
        return getBinaryStream( findColumn( columnName ) );
    }
    
    
    public SQLWarning getWarnings(){
        return null;
    }
    
    
    public void clearWarnings(){
        //TODO support for Warnings
    }
    
    
    public String getCursorName(){
        return null;
    }
    
    
    public ResultSetMetaData getMetaData(){
        return metaData;
    }
    
    
    public Object getObject(int columnIndex) throws SQLException {
        try{
            Object obj = getValue(columnIndex).getApiObject();
            wasNull = obj == null;
            return obj;
        }catch(Exception e){
            throw SmallSQLException.createFromException( e );
        }
    }
    public Object getObject(String columnName) throws SQLException {
        return getObject( findColumn( columnName ) );
    }
    
    
    public int findColumn(String columnName) throws SQLException {
    	return getCmd().findColumn(columnName) + 1;
    }
    

    public Reader getCharacterStream(int columnIndex) throws SQLException {
        /**@todo: Implement this java.sql.ResultSet.getCharacterStream method*/
        throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION, "getCharacterStream");
    }
    
    
    public Reader getCharacterStream(String columnName) throws SQLException {
        return getCharacterStream( findColumn( columnName ) );
    }
    
    
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        try{
            MutableNumeric obj = getValue(columnIndex).getNumeric();
            wasNull = obj == null;
            if(wasNull) return null;
            return obj.toBigDecimal();
        }catch(Exception e){
            throw SmallSQLException.createFromException( e );
        }
    }
    public BigDecimal getBigDecimal(String columnName) throws SQLException {
        return getBigDecimal( findColumn( columnName ) );
    }
    public boolean isBeforeFirst() throws SQLException {
		return getCmd().isBeforeFirst();
    }
    
    
    public boolean isAfterLast() throws SQLException {
        try{
            return getCmd().isAfterLast();
        }catch(Exception e){
            throw SmallSQLException.createFromException(e);
        }
    }
    
    
    public boolean isFirst() throws SQLException {
    	return getCmd().isFirst();
    }
    
    
    public boolean isLast() throws SQLException {
    	try{
    		return getCmd().isLast();
		}catch(Exception e){
			throw SmallSQLException.createFromException(e);
		}
    }
    
    
    public void beforeFirst() throws SQLException {
    	try{
            moveToCurrentRow();
    		getCmd().beforeFirst();
    	}catch(Exception e){
    		throw SmallSQLException.createFromException(e);
    	}
    }
    
    
    public boolean first() throws SQLException {
		try{
			if(st.rsType == ResultSet.TYPE_FORWARD_ONLY) throw SmallSQLException.create(Language.RSET_FWDONLY);
            moveToCurrentRow();
			return getCmd().first();
		}catch(Exception e){
			throw SmallSQLException.createFromException(e);
		}
    }
    
    
	public boolean previous() throws SQLException {
		try{
            moveToCurrentRow();
			return getCmd().previous();
		}catch(Exception e){
			throw SmallSQLException.createFromException(e);
		}
	}
    
    
	public boolean next() throws SQLException {
		try{
            moveToCurrentRow();
            return getCmd().next();
		}catch(Exception e){
			throw SmallSQLException.createFromException(e);
		}
	}
	
	
    public boolean last() throws SQLException {
		try{
            moveToCurrentRow();
            return getCmd().last();
		}catch(Exception e){
			throw SmallSQLException.createFromException(e);
		}
    }
    
    
	public void afterLast() throws SQLException {
		try{
			if(st.rsType == ResultSet.TYPE_FORWARD_ONLY) throw SmallSQLException.create(Language.RSET_FWDONLY);
            moveToCurrentRow();
            getCmd().afterLast();
		}catch(Exception e){
			throw SmallSQLException.createFromException(e);
		}
	}
    
    
    public boolean absolute(int row) throws SQLException {
		try{
            moveToCurrentRow();
			return getCmd().absolute(row);
		}catch(Exception e){
			throw SmallSQLException.createFromException(e);
		}
    }
    
    
    public boolean relative(int rows) throws SQLException {
		try{
            moveToCurrentRow();
			return getCmd().relative(rows);
		}catch(Exception e){
			throw SmallSQLException.createFromException(e);
		}
    }
    
    
	public int getRow() throws SQLException {
		try{
			return getCmd().getRow();
		}catch(Exception e){
			throw SmallSQLException.createFromException(e);
		}
	}
    
    
    public void setFetchDirection(int direction){
        fetchDirection = direction;
    }
    
    
    public int getFetchDirection(){
        return fetchDirection;
    }
    
    
    public void setFetchSize(int rows){
        fetchSize = rows;
    }
    
    
    public int getFetchSize(){
        return fetchSize;
    }
    
    
    public int getType() throws SQLException {
    	return getCmd().from.isScrollable() ? ResultSet.TYPE_SCROLL_SENSITIVE : ResultSet.TYPE_FORWARD_ONLY;
    }
    
    
    public int getConcurrency(){
    	return isUpdatable ? ResultSet.CONCUR_UPDATABLE : ResultSet.CONCUR_READ_ONLY;
    }
    
    
    public boolean rowUpdated(){
    	return false;
    }
    
    
    public boolean rowInserted() throws SQLException {
    	return getCmd().from.rowInserted();
    }
    
    
    public boolean rowDeleted() throws SQLException {
    	return getCmd().from.rowDeleted();
    }
    
    
    public void updateNull(int columnIndex) throws SQLException {
		updateValue( columnIndex, null, SQLTokenizer.NULL);
    }
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		updateValue( columnIndex, x ? Boolean.TRUE : Boolean.FALSE, SQLTokenizer.BOOLEAN);
    }
    public void updateByte(int columnIndex, byte x) throws SQLException {
		updateValue( columnIndex, Utils.getShort(x), SQLTokenizer.TINYINT);
    }
    public void updateShort(int columnIndex, short x) throws SQLException {
		updateValue( columnIndex, Utils.getShort(x), SQLTokenizer.SMALLINT);
    }
    public void updateInt(int columnIndex, int x) throws SQLException {
		updateValue( columnIndex, Utils.getInteger(x), SQLTokenizer.INT);
    }
    public void updateLong(int columnIndex, long x) throws SQLException {
		updateValue( columnIndex, new Long(x), SQLTokenizer.BIGINT);
    }
    public void updateFloat(int columnIndex, float x) throws SQLException {
		updateValue( columnIndex, new Float(x), SQLTokenizer.REAL);
    }
    public void updateDouble(int columnIndex, double x) throws SQLException {
		updateValue( columnIndex, new Double(x), SQLTokenizer.DOUBLE);
    }
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
		updateValue( columnIndex, x, SQLTokenizer.DECIMAL);
    }
    public void updateString(int columnIndex, String x) throws SQLException {
		updateValue( columnIndex, x, SQLTokenizer.VARCHAR);
    }
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
		updateValue( columnIndex, x, SQLTokenizer.VARBINARY);
    }
    public void updateDate(int columnIndex, Date x) throws SQLException {
		updateValue( columnIndex, DateTime.valueOf(x), SQLTokenizer.DATE);
    }
    public void updateTime(int columnIndex, Time x) throws SQLException {
		updateValue( columnIndex, DateTime.valueOf(x), SQLTokenizer.TIME);
    }
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
		updateValue( columnIndex, DateTime.valueOf(x), SQLTokenizer.TIMESTAMP);
    }
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
		updateValue( columnIndex, x, SQLTokenizer.LONGVARCHAR, length);
    }
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
		updateValue( columnIndex, x, SQLTokenizer.LONGVARBINARY, length);
    }
    
    
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        /**@todo: Implement this java.sql.ResultSet.updateCharacterStream method*/
        throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION, "Reader object");
    }
    
    
    public void updateObject(int columnIndex, Object x, int scale) throws SQLException {
    	//TODO scale to consider
		updateValue( columnIndex, x, -1);
    }
    
    
    public void updateObject(int columnIndex, Object x) throws SQLException {
    	updateValue( columnIndex, x, -1);
    }
    public void updateNull(String columnName) throws SQLException {
        updateNull( findColumn( columnName ) );
    }
    public void updateBoolean(String columnName, boolean x) throws SQLException {
        updateBoolean( findColumn( columnName ), x );
    }
    public void updateByte(String columnName, byte x) throws SQLException {
        updateByte( findColumn( columnName ), x );
    }
    public void updateShort(String columnName, short x) throws SQLException {
        updateShort( findColumn( columnName ), x );
    }
    public void updateInt(String columnName, int x) throws SQLException {
        updateInt( findColumn( columnName ), x );
    }
    public void updateLong(String columnName, long x) throws SQLException {
        updateLong( findColumn( columnName ), x );
    }
    public void updateFloat(String columnName, float x) throws SQLException {
        updateFloat( findColumn( columnName ), x );
    }
    public void updateDouble(String columnName, double x) throws SQLException {
        updateDouble( findColumn( columnName ), x );
    }
    public void updateBigDecimal(String columnName, BigDecimal x) throws SQLException {
        updateBigDecimal( findColumn( columnName ), x );
    }
    public void updateString(String columnName, String x) throws SQLException {
        updateString( findColumn( columnName ), x );
    }
    public void updateBytes(String columnName, byte[] x) throws SQLException {
        updateBytes( findColumn( columnName ), x );
    }
    public void updateDate(String columnName, Date x) throws SQLException {
        updateDate( findColumn( columnName ), x );
    }
    public void updateTime(String columnName, Time x) throws SQLException {
        updateTime( findColumn( columnName ), x );
    }
    public void updateTimestamp(String columnName, Timestamp x) throws SQLException {
        updateTimestamp( findColumn( columnName ), x );
    }
    public void updateAsciiStream(String columnName, InputStream x, int length) throws SQLException {
        updateAsciiStream( findColumn( columnName ), x, length );
    }
    public void updateBinaryStream(String columnName, InputStream x, int length) throws SQLException {
        updateBinaryStream( findColumn( columnName ), x, length );
    }
    public void updateCharacterStream(String columnName, Reader x, int length) throws SQLException {
        updateCharacterStream( findColumn( columnName ), x, length );
    }
    public void updateObject(String columnName, Object x, int scale) throws SQLException {
        updateObject( findColumn( columnName ), x, scale );
    }
    public void updateObject(String columnName, Object x) throws SQLException {
        updateObject( findColumn( columnName ), x );
    }
    
    public void insertRow() throws SQLException {
		st.con.log.println("insertRow()");
        if(!isInsertRow){
            throw SmallSQLException.create(Language.RSET_NOT_INSERT_ROW);
        }
		getCmd().insertRow( st.con, values);
        clearRowBuffer();
    }
    
    
    /**
     * Test if it on the insert row.
     * @throws SQLException if on the insert row
     */
    private void testNotInsertRow() throws SQLException{
        if(isInsertRow){
            throw SmallSQLException.create(Language.RSET_ON_INSERT_ROW);
        }
    }
    
    public void updateRow() throws SQLException {
        try {
        	if(values == null){
                // no changes then also no update needed
                return;
            }
       		st.con.log.println("updateRow()");
            testNotInsertRow();
            final CommandSelect command = getCmd();
            command.updateRow( st.con, values);
            command.relative(0);  //refresh the row
            clearRowBuffer();
        } catch (Exception e) {
            throw SmallSQLException.createFromException(e);
        }
    }
    
    
    public void deleteRow() throws SQLException {
		st.con.log.println("deleteRow()");
        testNotInsertRow();
    	getCmd().deleteRow(st.con);
        clearRowBuffer();
    }
    public void refreshRow() throws SQLException {
        testNotInsertRow();
        relative(0);
    }
    

    public void cancelRowUpdates() throws SQLException{
        testNotInsertRow();
        clearRowBuffer();
    }
    
    
    /**
     * Clear the update row or insert row buffer.
     */
    private void clearRowBuffer(){
        if(values != null){
            for(int i=values.length-1; i>=0; i--){
                values[i].clear();
            }
        }
    }
    

    public void moveToInsertRow() throws SQLException {
    	if(isUpdatable){
    		isInsertRow = true;
            clearRowBuffer();
    	}else{
            throw SmallSQLException.create(Language.RSET_READONLY);
    	}
    }
    
    
    public void moveToCurrentRow() throws SQLException{
		isInsertRow = false;
        clearRowBuffer();
        if(values == null){
            //init the values array as insert row buffer 
            getUpdateValue(1);
        }
    }
    
    
    public Statement getStatement() {
        return st;
    }
    
    
    public Object getObject(int i, Map map) throws SQLException {
        return getObject( i );
    }
    
    
    public Ref getRef(int i) throws SQLException {
        /**@todo: Implement this java.sql.ResultSet.getRef method*/
        throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION, "Ref object");
    }
    
    
    public Blob getBlob(int i) throws SQLException {
        /**@todo: Implement this java.sql.ResultSet.getBlob method*/
        throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION, "Blob object");
    }
    
    
    public Clob getClob(int i) throws SQLException {
        /**@todo: Implement this java.sql.ResultSet.getClob method*/
        throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION, "Clob object");
    }
    
    
    public Array getArray(int i) throws SQLException {
        /**@todo: Implement this java.sql.ResultSet.getArray method*/
        throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION, "Array");
    }
    
    
    public Object getObject(String columnName, Map map) throws SQLException {
        return getObject( columnName );
    }
    public Ref getRef(String columnName) throws SQLException {
        return getRef( findColumn( columnName ) );
    }
    public Blob getBlob(String columnName) throws SQLException {
        return getBlob( findColumn( columnName ) );
    }
    public Clob getClob(String columnName) throws SQLException {
        return getClob( findColumn( columnName ) );
    }
    public Array getArray(String columnName) throws SQLException {
        return getArray( findColumn( columnName ) );
    }
    
    
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        try{
            if(cal == null){
                return getDate(columnIndex);
            }
            Expression expr = getValue(columnIndex);
            wasNull = expr.isNull();
            if(wasNull) return null;
            return new Date(DateTime.addDateTimeOffset( expr.getLong(), cal.getTimeZone() ));
        }catch(Exception e){
            throw SmallSQLException.createFromException( e );
        }
    }
    
    
    public Date getDate(String columnName, Calendar cal) throws SQLException {
        return getDate( findColumn( columnName ), cal );
    }
    
    
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        try{
            if(cal == null){
                return getTime(columnIndex);
            }
            Expression expr = getValue(columnIndex);
            wasNull = expr.isNull();
            if(wasNull) return null;
            return new Time(DateTime.addDateTimeOffset( expr.getLong(), cal.getTimeZone() ));
        }catch(Exception e){
            throw SmallSQLException.createFromException( e );
        }
    }
    
    
    public Time getTime(String columnName, Calendar cal) throws SQLException {
        return getTime( findColumn( columnName ), cal );
    }
    
    
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        try{
            if(cal == null){
                return getTimestamp(columnIndex);
            }
            Expression expr = getValue(columnIndex);
            wasNull = expr.isNull();
            if(wasNull) return null;
            return new Timestamp(DateTime.addDateTimeOffset( expr.getLong(), cal.getTimeZone() ));
        }catch(Exception e){
            throw SmallSQLException.createFromException( e );
        }
    }
    
    
    public Timestamp getTimestamp(String columnName, Calendar cal) throws SQLException {
        return getTimestamp( findColumn( columnName ), cal );
    }
    
    
    public URL getURL(int columnIndex) throws SQLException {
        try{
            Expression expr = getValue(columnIndex);
            wasNull = expr.isNull();
            if(wasNull) return null;
            return new URL( expr.getString() );
        }catch(Exception e){
            throw SmallSQLException.createFromException( e );
        }
    }
    
    
    public URL getURL(String columnName) throws SQLException {
        return getURL( findColumn( columnName ) );
    }
    
    
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        /**@todo: Implement this java.sql.ResultSet.updateRef method*/
        throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION, "Ref");
    }
    
    
    public void updateRef(String columnName, Ref x) throws SQLException {
        updateRef( findColumn( columnName ), x );
    }
    
    
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        /**@todo: Implement this java.sql.ResultSet.updateBlob method*/
        throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION, "Blob");
    }
    
    
    public void updateBlob(String columnName, Blob x) throws SQLException {
        updateBlob( findColumn( columnName ), x );
    }
    
    
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        /**@todo: Implement this java.sql.ResultSet.updateClob method*/
        throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION, "Clob");
    }
    
    
    public void updateClob(String columnName, Clob x) throws SQLException {
        updateClob( findColumn( columnName ), x );
    }
    
    
    public void updateArray(int columnIndex, Array x) throws SQLException {
        /**@todo: Implement this java.sql.ResultSet.updateArray method*/
        throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION, "Array");
    }
    
    
    public void updateArray(String columnName, Array x) throws SQLException {
        updateArray( findColumn( columnName ), x );
    }
    
	/*========================================================

	private methods

	=========================================================*/

    /**
     * Get the expression of a column. 
     * This expression can be used to request a value of the current row.
     */
    final private Expression getValue(int columnIndex) throws SQLException{
        if(values != null){
            ExpressionValue value = values[ metaData.getColumnIdx( columnIndex ) ];
            if(!value.isEmpty() || isInsertRow){ 
                return value;
            }
        }
        return metaData.getColumnExpression(columnIndex);
    }
    

	final private ExpressionValue getUpdateValue(int columnIndex) throws SQLException{
		if(values == null){
			int count = metaData.getColumnCount();
			values = new ExpressionValue[count];
			while(count-- > 0){
				values[count] = new ExpressionValue();
			}
		}
		return values[ metaData.getColumnIdx( columnIndex ) ];
	}
	
    
    final private void updateValue(int columnIndex, Object x, int dataType) throws SQLException{
		getUpdateValue( columnIndex ).set( x, dataType );
		if(st.con.log.isLogging()){
			
			st.con.log.println("parameter '"+metaData.getColumnName(columnIndex)+"' = "+x+"; type="+dataType);
		}
    }
    
    
	final private void updateValue(int columnIndex, Object x, int dataType, int length) throws SQLException{
		getUpdateValue( columnIndex ).set( x, dataType, length );
		if(st.con.log.isLogging()){
			st.con.log.println("parameter '"+metaData.getColumnName(columnIndex)+"' = "+x+"; type="+dataType+"; length="+length);
		}
	}


	final private CommandSelect getCmd() throws SQLException {
		if(cmd == null){
            throw SmallSQLException.create(Language.RSET_CLOSED);
        }
        st.con.testClosedConnection();
		return cmd;
	}
}