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

import smallsql.tools.util;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;


class SSPreparedStatement extends SSStatement implements PreparedStatement {

    private ArrayList batches;
    private final int top; // value of an optional top expression

    SSPreparedStatement(SSConnection con, String sql) throws SQLException {
        this(con, sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    SSPreparedStatement(SSConnection con, String sql, int rsType, int rsConcurrency) throws SQLException {
        super(con, rsType, rsConcurrency);
        con.log.println(sql);
        SQLParser parser = new SQLParser();
        cmd = parser.parse(con, sql);
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
        if (getMaxRows() != 0 && (top == -1 || top > getMaxRows()))
            cmd.setMaxRows(getMaxRows());
        cmd.execute(con, this);
    }

    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        checkStatement();
        cmd.setParamValue(parameterIndex, null, SQLTokenizer.NULL);
    }


    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        checkStatement();
        cmd.setParamValue(parameterIndex, x ? Boolean.TRUE : Boolean.FALSE, SQLTokenizer.BOOLEAN);
    }


    public void setByte(int parameterIndex, byte x) throws SQLException {
        checkStatement();
        cmd.setParamValue(parameterIndex, new Integer(x), SQLTokenizer.TINYINT);
    }


    public void setShort(int parameterIndex, short x) throws SQLException {
        checkStatement();
        cmd.setParamValue(parameterIndex, new Integer(x), SQLTokenizer.SMALLINT);
    }


    public void setInt(int parameterIndex, int x) throws SQLException {
        checkStatement();
        cmd.setParamValue(parameterIndex, new Integer(x), SQLTokenizer.INT);
    }


    public void setLong(int parameterIndex, long x) throws SQLException {
        checkStatement();
        cmd.setParamValue(parameterIndex, new Long(x), SQLTokenizer.BIGINT);
    }


    public void setFloat(int parameterIndex, float x) throws SQLException {
        checkStatement();
        cmd.setParamValue(parameterIndex, new Float(x), SQLTokenizer.REAL);
    }


    public void setDouble(int parameterIndex, double x) throws SQLException {
        checkStatement();
        cmd.setParamValue(parameterIndex, new Double(x), SQLTokenizer.DOUBLE);
    }


    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        checkStatement();
        cmd.setParamValue(parameterIndex, x, SQLTokenizer.DECIMAL);
    }


    public void setString(int parameterIndex, String x) throws SQLException {
        checkStatement();
        cmd.setParamValue(parameterIndex, x, SQLTokenizer.VARCHAR);
    }


    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        checkStatement();
        cmd.setParamValue(parameterIndex, x, SQLTokenizer.BINARY);
    }


    public void setDate(int parameterIndex, Date x) throws SQLException {
        checkStatement();
        cmd.setParamValue(parameterIndex, DateTime.valueOf(x), SQLTokenizer.DATE);
    }


    public void setTime(int parameterIndex, Time x) throws SQLException {
        checkStatement();
        cmd.setParamValue(parameterIndex, DateTime.valueOf(x), SQLTokenizer.TIME);
    }


    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        checkStatement();
        cmd.setParamValue(parameterIndex, DateTime.valueOf(x), SQLTokenizer.TIMESTAMP);
    }


    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        checkStatement();
        cmd.setParamValue(parameterIndex, x, SQLTokenizer.LONGVARCHAR, length);
    }


    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        checkStatement();
        /**@todo: Implement this java.sql.PreparedStatement method*/
        throw new java.lang.UnsupportedOperationException("Method setUnicodeStream() not yet implemented.");
    }


    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        checkStatement();
        cmd.setParamValue(parameterIndex, x, SQLTokenizer.LONGVARBINARY, length);
    }


    public void clearParameters() throws SQLException {
        checkStatement();
        cmd.clearParams();
    }


    public void setObject(int parameterIndex, Object x, int targetSqlType, int scale) throws SQLException {
        checkStatement();
        //FIXME Scale to consider
        cmd.setParamValue(parameterIndex, x, -1);
    }

    /**
     * Sets the designated parameter to the given input stream, which will have
     * the specified number of bytes.
     * When a very large ASCII value is input to a <code>LONGVARCHAR</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.InputStream</code>. Data will be read from the stream
     * as needed until end-of-file is reached.  The JDBC driver will
     * do any necessary conversion from ASCII to the database char format.
     *
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the Java input stream that contains the ASCII parameter value
     * @param length         the number of bytes in the stream
     * @throws SQLException if parameterIndex does not correspond to a parameter
     *                      marker in the SQL statement; if a database access error occurs or
     *                      this method is called on a closed <code>PreparedStatement</code>
     * @since 1.6
     */
    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw util.generateUnsupportedOperation();
    }

    /**
     * Sets the designated parameter to the given input stream, which will have
     * the specified number of bytes.
     * When a very large binary value is input to a <code>LONGVARBINARY</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.InputStream</code> object. The data will be read from the
     * stream as needed until end-of-file is reached.
     *
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the java input stream which contains the binary parameter value
     * @param length         the number of bytes in the stream
     * @throws SQLException if parameterIndex does not correspond to a parameter
     *                      marker in the SQL statement; if a database access error occurs or
     *                      this method is called on a closed <code>PreparedStatement</code>
     * @since 1.6
     */
    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw util.generateUnsupportedOperation();
    }

    /**
     * Sets the designated parameter to the given <code>Reader</code>
     * object, which is the given number of characters long.
     * When a very large UNICODE value is input to a <code>LONGVARCHAR</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.Reader</code> object. The data will be read from the stream
     * as needed until end-of-file is reached.  The JDBC driver will
     * do any necessary conversion from UNICODE to the database char format.
     *
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param reader         the <code>java.io.Reader</code> object that contains the
     *                       Unicode data
     * @param length         the number of characters in the stream
     * @throws SQLException if parameterIndex does not correspond to a parameter
     *                      marker in the SQL statement; if a database access error occurs or
     *                      this method is called on a closed <code>PreparedStatement</code>
     * @since 1.6
     */
    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw util.generateUnsupportedOperation();
    }

    /**
     * Sets the designated parameter to the given input stream.
     * When a very large ASCII value is input to a <code>LONGVARCHAR</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.InputStream</code>. Data will be read from the stream
     * as needed until end-of-file is reached.  The JDBC driver will
     * do any necessary conversion from ASCII to the database char format.
     *
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * <code>setAsciiStream</code> which takes a length parameter.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the Java input stream that contains the ASCII parameter value
     * @throws SQLException                    if parameterIndex does not correspond to a parameter
     *                                         marker in the SQL statement; if a database access error occurs or
     *                                         this method is called on a closed <code>PreparedStatement</code>
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support this method
     * @since 1.6
     */
    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw util.generateUnsupportedOperation();
    }

    /**
     * Sets the designated parameter to the given input stream.
     * When a very large binary value is input to a <code>LONGVARBINARY</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.InputStream</code> object. The data will be read from the
     * stream as needed until end-of-file is reached.
     *
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * <code>setBinaryStream</code> which takes a length parameter.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the java input stream which contains the binary parameter value
     * @throws SQLException                    if parameterIndex does not correspond to a parameter
     *                                         marker in the SQL statement; if a database access error occurs or
     *                                         this method is called on a closed <code>PreparedStatement</code>
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support this method
     * @since 1.6
     */
    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw util.generateUnsupportedOperation();
    }

    /**
     * Sets the designated parameter to the given <code>Reader</code>
     * object.
     * When a very large UNICODE value is input to a <code>LONGVARCHAR</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.Reader</code> object. The data will be read from the stream
     * as needed until end-of-file is reached.  The JDBC driver will
     * do any necessary conversion from UNICODE to the database char format.
     *
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * <code>setCharacterStream</code> which takes a length parameter.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param reader         the <code>java.io.Reader</code> object that contains the
     *                       Unicode data
     * @throws SQLException                    if parameterIndex does not correspond to a parameter
     *                                         marker in the SQL statement; if a database access error occurs or
     *                                         this method is called on a closed <code>PreparedStatement</code>
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support this method
     * @since 1.6
     */
    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw util.generateUnsupportedOperation();
    }

    /**
     * Sets the designated parameter to a <code>Reader</code> object. The
     * <code>Reader</code> reads the data till end-of-file is reached. The
     * driver does the necessary conversion from Java character format to
     * the national character set in the database.
     *
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * <code>setNCharacterStream</code> which takes a length parameter.
     *
     * @param parameterIndex of the first parameter is 1, the second is 2, ...
     * @param value          the parameter value
     * @throws SQLException                    if parameterIndex does not correspond to a parameter
     *                                         marker in the SQL statement; if the driver does not support national
     *                                         character sets;  if the driver can detect that a data conversion
     *                                         error could occur; if a database access error occurs; or
     *                                         this method is called on a closed <code>PreparedStatement</code>
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support this method
     * @since 1.6
     */
    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw util.generateUnsupportedOperation();
    }

    /**
     * Sets the designated parameter to a <code>Reader</code> object.
     * This method differs from the <code>setCharacterStream (int, Reader)</code> method
     * because it informs the driver that the parameter value should be sent to
     * the server as a <code>CLOB</code>.  When the <code>setCharacterStream</code> method is used, the
     * driver may have to do extra work to determine whether the parameter
     * data should be sent to the server as a <code>LONGVARCHAR</code> or a <code>CLOB</code>
     *
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * <code>setClob</code> which takes a length parameter.
     *
     * @param parameterIndex index of the first parameter is 1, the second is 2, ...
     * @param reader         An object that contains the data to set the parameter value to.
     * @throws SQLException                    if parameterIndex does not correspond to a parameter
     *                                         marker in the SQL statement; if a database access error occurs; this method is called on
     *                                         a closed <code>PreparedStatement</code>or if parameterIndex does not correspond to a parameter
     *                                         marker in the SQL statement
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support this method
     * @since 1.6
     */
    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw util.generateUnsupportedOperation();
    }

    /**
     * Sets the designated parameter to a <code>InputStream</code> object.
     * This method differs from the <code>setBinaryStream (int, InputStream)</code>
     * method because it informs the driver that the parameter value should be
     * sent to the server as a <code>BLOB</code>.  When the <code>setBinaryStream</code> method is used,
     * the driver may have to do extra work to determine whether the parameter
     * data should be sent to the server as a <code>LONGVARBINARY</code> or a <code>BLOB</code>
     *
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * <code>setBlob</code> which takes a length parameter.
     *
     * @param parameterIndex index of the first parameter is 1,
     *                       the second is 2, ...
     * @param inputStream    An object that contains the data to set the parameter
     *                       value to.
     * @throws SQLException                    if parameterIndex does not correspond to a parameter
     *                                         marker in the SQL statement; if a database access error occurs;
     *                                         this method is called on a closed <code>PreparedStatement</code> or
     *                                         if parameterIndex does not correspond
     *                                         to a parameter marker in the SQL statement,
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support this method
     * @since 1.6
     */
    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw util.generateUnsupportedOperation();
    }

    /**
     * Sets the designated parameter to a <code>Reader</code> object.
     * This method differs from the <code>setCharacterStream (int, Reader)</code> method
     * because it informs the driver that the parameter value should be sent to
     * the server as a <code>NCLOB</code>.  When the <code>setCharacterStream</code> method is used, the
     * driver may have to do extra work to determine whether the parameter
     * data should be sent to the server as a <code>LONGNVARCHAR</code> or a <code>NCLOB</code>
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * <code>setNClob</code> which takes a length parameter.
     *
     * @param parameterIndex index of the first parameter is 1, the second is 2, ...
     * @param reader         An object that contains the data to set the parameter value to.
     * @throws SQLException                    if parameterIndex does not correspond to a parameter
     *                                         marker in the SQL statement;
     *                                         if the driver does not support national character sets;
     *                                         if the driver can detect that a data conversion
     *                                         error could occur;  if a database access error occurs or
     *                                         this method is called on a closed <code>PreparedStatement</code>
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support this method
     * @since 1.6
     */
    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw util.generateUnsupportedOperation();
    }


    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        checkStatement();
        cmd.setParamValue(parameterIndex, x, -1);
    }


    public void setObject(int parameterIndex, Object x) throws SQLException {
        checkStatement();
        cmd.setParamValue(parameterIndex, x, -1);
    }


    public boolean execute() throws SQLException {
        executeImp();
        return cmd.getResultSet() != null;
    }


    public void addBatch() throws SQLException {
        checkStatement();
        try {
            final Expressions params = cmd.params;
            final int size = params.size();
            ExpressionValue[] values = new ExpressionValue[size];
            for (int i = 0; i < size; i++) {
                values[i] = (ExpressionValue) params.get(i).clone();
            }
            if (batches == null) batches = new ArrayList();
            batches.add(values);
        } catch (Exception e) {
            throw SmallSQLException.createFromException(e);
        }
    }


    public void clearBatch() throws SQLException {
        checkStatement();
        if (batches != null) batches.clear();
    }


    public int[] executeBatch() throws BatchUpdateException {
        if (batches == null || batches.size() == 0) return new int[0];
        int[] result = new int[batches.size()];
        BatchUpdateException failed = null;
        for (int b = 0; b < batches.size(); b++) {
            try {
                checkStatement();
                ExpressionValue[] values = (ExpressionValue[]) batches.get(b);
                for (int i = 0; i < values.length; i++) {
                    ((ExpressionValue) cmd.params.get(i)).set(values[i]);
                }
                result[b] = executeUpdate();
            } catch (SQLException ex) {
                result[b] = EXECUTE_FAILED;
                if (failed == null) {
                    failed = new BatchUpdateException(ex.getMessage(), ex.getSQLState(), ex.getErrorCode(), result);
                    failed.initCause(ex);
                }
                failed.setNextException(ex);
            }
        }
        batches.clear();
        if (failed != null)
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
        if (cmd instanceof CommandSelect) {
            try {
                ((CommandSelect) cmd).compile(con);
                SSResultSetMetaData metaData = new SSResultSetMetaData();
                metaData.columns = cmd.columnExpressions;
                return metaData;
            } catch (Exception e) {
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

    /**
     * Sets the designated parameter to the given <code>java.sql.RowId</code> object. The
     * driver converts this to a SQL <code>ROWID</code> value when it sends it
     * to the database
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the parameter value
     * @throws SQLException                    if parameterIndex does not correspond to a parameter
     *                                         marker in the SQL statement; if a database access error occurs or
     *                                         this method is called on a closed <code>PreparedStatement</code>
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support this method
     * @since 1.6
     */
    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw util.generateUnsupportedOperation();
    }

    /**
     * Sets the designated parameter to the given <code>String</code> object.
     * The driver converts this to a SQL <code>NCHAR</code> or
     * <code>NVARCHAR</code> or <code>LONGNVARCHAR</code> value
     * (depending on the argument's
     * size relative to the driver's limits on <code>NVARCHAR</code> values)
     * when it sends it to the database.
     *
     * @param parameterIndex of the first parameter is 1, the second is 2, ...
     * @param value          the parameter value
     * @throws SQLException                    if parameterIndex does not correspond to a parameter
     *                                         marker in the SQL statement; if the driver does not support national
     *                                         character sets;  if the driver can detect that a data conversion
     *                                         error could occur; if a database access error occurs; or
     *                                         this method is called on a closed <code>PreparedStatement</code>
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support this method
     * @since 1.6
     */
    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        throw util.generateUnsupportedOperation();
    }

    /**
     * Sets the designated parameter to a <code>Reader</code> object. The
     * <code>Reader</code> reads the data till end-of-file is reached. The
     * driver does the necessary conversion from Java character format to
     * the national character set in the database.
     *
     * @param parameterIndex of the first parameter is 1, the second is 2, ...
     * @param value          the parameter value
     * @param length         the number of characters in the parameter data.
     * @throws SQLException                    if parameterIndex does not correspond to a parameter
     *                                         marker in the SQL statement; if the driver does not support national
     *                                         character sets;  if the driver can detect that a data conversion
     *                                         error could occur; if a database access error occurs; or
     *                                         this method is called on a closed <code>PreparedStatement</code>
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support this method
     * @since 1.6
     */
    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw util.generateUnsupportedOperation();
    }

    /**
     * Sets the designated parameter to a <code>java.sql.NClob</code> object. The driver converts this to a
     * SQL <code>NCLOB</code> value when it sends it to the database.
     *
     * @param parameterIndex of the first parameter is 1, the second is 2, ...
     * @param value          the parameter value
     * @throws SQLException                    if parameterIndex does not correspond to a parameter
     *                                         marker in the SQL statement; if the driver does not support national
     *                                         character sets;  if the driver can detect that a data conversion
     *                                         error could occur; if a database access error occurs; or
     *                                         this method is called on a closed <code>PreparedStatement</code>
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support this method
     * @since 1.6
     */
    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw util.generateUnsupportedOperation();
    }

    /**
     * Sets the designated parameter to a <code>Reader</code> object.  The reader must contain  the number
     * of characters specified by length otherwise a <code>SQLException</code> will be
     * generated when the <code>PreparedStatement</code> is executed.
     * This method differs from the <code>setCharacterStream (int, Reader, int)</code> method
     * because it informs the driver that the parameter value should be sent to
     * the server as a <code>CLOB</code>.  When the <code>setCharacterStream</code> method is used, the
     * driver may have to do extra work to determine whether the parameter
     * data should be sent to the server as a <code>LONGVARCHAR</code> or a <code>CLOB</code>
     *
     * @param parameterIndex index of the first parameter is 1, the second is 2, ...
     * @param reader         An object that contains the data to set the parameter value to.
     * @param length         the number of characters in the parameter data.
     * @throws SQLException                    if parameterIndex does not correspond to a parameter
     *                                         marker in the SQL statement; if a database access error occurs; this method is called on
     *                                         a closed <code>PreparedStatement</code> or if the length specified is less than zero.
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support this method
     * @since 1.6
     */
    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw util.generateUnsupportedOperation();
    }

    /**
     * Sets the designated parameter to a <code>InputStream</code> object.
     * The {@code Inputstream} must contain  the number
     * of characters specified by length otherwise a <code>SQLException</code> will be
     * generated when the <code>PreparedStatement</code> is executed.
     * This method differs from the <code>setBinaryStream (int, InputStream, int)</code>
     * method because it informs the driver that the parameter value should be
     * sent to the server as a <code>BLOB</code>.  When the <code>setBinaryStream</code> method is used,
     * the driver may have to do extra work to determine whether the parameter
     * data should be sent to the server as a <code>LONGVARBINARY</code> or a <code>BLOB</code>
     *
     * @param parameterIndex index of the first parameter is 1,
     *                       the second is 2, ...
     * @param inputStream    An object that contains the data to set the parameter
     *                       value to.
     * @param length         the number of bytes in the parameter data.
     * @throws SQLException                    if parameterIndex does not correspond to a parameter
     *                                         marker in the SQL statement; if a database access error occurs;
     *                                         this method is called on a closed <code>PreparedStatement</code>;
     *                                         if the length specified
     *                                         is less than zero or if the number of bytes in the {@code InputStream} does not match
     *                                         the specified length.
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support this method
     * @since 1.6
     */
    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw util.generateUnsupportedOperation();
    }

    /**
     * Sets the designated parameter to a <code>Reader</code> object.  The reader must contain  the number
     * of characters specified by length otherwise a <code>SQLException</code> will be
     * generated when the <code>PreparedStatement</code> is executed.
     * This method differs from the <code>setCharacterStream (int, Reader, int)</code> method
     * because it informs the driver that the parameter value should be sent to
     * the server as a <code>NCLOB</code>.  When the <code>setCharacterStream</code> method is used, the
     * driver may have to do extra work to determine whether the parameter
     * data should be sent to the server as a <code>LONGNVARCHAR</code> or a <code>NCLOB</code>
     *
     * @param parameterIndex index of the first parameter is 1, the second is 2, ...
     * @param reader         An object that contains the data to set the parameter value to.
     * @param length         the number of characters in the parameter data.
     * @throws SQLException                    if parameterIndex does not correspond to a parameter
     *                                         marker in the SQL statement; if the length specified is less than zero;
     *                                         if the driver does not support national character sets;
     *                                         if the driver can detect that a data conversion
     *                                         error could occur;  if a database access error occurs or
     *                                         this method is called on a closed <code>PreparedStatement</code>
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support this method
     * @since 1.6
     */
    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw util.generateUnsupportedOperation();
    }

    /**
     * Sets the designated parameter to the given <code>java.sql.SQLXML</code> object.
     * The driver converts this to an
     * SQL <code>XML</code> value when it sends it to the database.
     *
     * @param parameterIndex index of the first parameter is 1, the second is 2, ...
     * @param xmlObject      a <code>SQLXML</code> object that maps an SQL <code>XML</code> value
     * @throws SQLException                    if parameterIndex does not correspond to a parameter
     *                                         marker in the SQL statement; if a database access error occurs;
     *                                         this method is called on a closed <code>PreparedStatement</code>
     *                                         or the <code>java.xml.transform.Result</code>,
     *                                         <code>Writer</code> or <code>OutputStream</code> has not been closed for
     *                                         the <code>SQLXML</code> object
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support this method
     * @since 1.6
     */
    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw util.generateUnsupportedOperation();
    }
}
