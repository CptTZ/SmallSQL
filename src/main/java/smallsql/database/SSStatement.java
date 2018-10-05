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
 * SSStatement.java
 * ---------------
 * Author: Volker Berlin
 *
 */
package smallsql.database;

import smallsql.database.language.Language;
import smallsql.tools.util;

import java.sql.*;
import java.util.ArrayList;

class SSStatement implements Statement {

    final SSConnection con;

    Command cmd;

    private boolean isClosed;

    int rsType;

    int rsConcurrency;

    private int fetchDirection;

    private int fetchSize;

    private int queryTimeout;

    private int maxRows;

    private int maxFieldSize;

    private ArrayList batches;

    private boolean needGeneratedKeys;

    private ResultSet generatedKeys;

    private int[] generatedKeyIndexes;

    private String[] generatedKeyNames;


    SSStatement(SSConnection con) throws SQLException {
        this(con, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }


    SSStatement(SSConnection con, int rsType, int rsConcurrency) throws SQLException {
        this.con = con;
        this.rsType = rsType;
        this.rsConcurrency = rsConcurrency;
        con.testClosedConnection();
    }


    final public ResultSet executeQuery(String sql) throws SQLException {
        executeImpl(sql);
        return cmd.getQueryResult();
    }


    final public int executeUpdate(String sql) throws SQLException {
        executeImpl(sql);
        return cmd.getUpdateCount();
    }


    final public boolean execute(String sql) throws SQLException {
        executeImpl(sql);
        return cmd.getResultSet() != null;
    }


    final private void executeImpl(String sql) throws SQLException {
        checkStatement();
        generatedKeys = null;
        try {
            con.log.println(sql);
            SQLParser parser = new SQLParser();
            cmd = parser.parse(con, sql);
            if (maxRows != 0 && (cmd.getMaxRows() == -1 || cmd.getMaxRows() > maxRows))
                cmd.setMaxRows(maxRows);
            cmd.execute(con, this);
        } catch (Exception e) {
            throw SmallSQLException.createFromException(e);
        }
        needGeneratedKeys = false;
        generatedKeyIndexes = null;
        generatedKeyNames = null;
    }


    final public void close() {
        con.log.println("Statement.close");
        isClosed = true;
        cmd = null;
        // TODO make Resources free;
    }


    final public int getMaxFieldSize() {
        return maxFieldSize;
    }


    final public void setMaxFieldSize(int max) {
        maxFieldSize = max;
    }


    final public int getMaxRows() {
        return maxRows;
    }


    final public void setMaxRows(int max) throws SQLException {
        if (max < 0)
            throw SmallSQLException.create(Language.ROWS_WRONG_MAX, String.valueOf(max));
        maxRows = max;
    }


    final public void setEscapeProcessing(boolean enable) throws SQLException {
        checkStatement();
        // TODO enable/disable escape processing
    }


    final public int getQueryTimeout() throws SQLException {
        checkStatement();
        return queryTimeout;
    }


    final public void setQueryTimeout(int seconds) throws SQLException {
        checkStatement();
        queryTimeout = seconds;
    }


    final public void cancel() throws SQLException {
        checkStatement();
        // TODO Statement.cancel()
    }


    final public SQLWarning getWarnings() {
        return null;
    }


    final public void clearWarnings() {
        // TODO support for warnings
    }


    final public void setCursorName(String name) throws SQLException {
        /** @todo: Implement this java.sql.Statement.setCursorName method */
        throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION, "setCursorName");
    }


    final public ResultSet getResultSet() throws SQLException {
        checkStatement();
        return cmd.getResultSet();
    }


    final public int getUpdateCount() throws SQLException {
        checkStatement();
        return cmd.getUpdateCount();
    }


    final public boolean getMoreResults() throws SQLException {
        checkStatement();
        return getMoreResults(CLOSE_CURRENT_RESULT);
    }


    final public void setFetchDirection(int direction) throws SQLException {
        checkStatement();
        fetchDirection = direction;
    }


    final public int getFetchDirection() throws SQLException {
        checkStatement();
        return fetchDirection;
    }


    final public void setFetchSize(int rows) throws SQLException {
        checkStatement();
        fetchSize = rows;
    }


    final public int getFetchSize() throws SQLException {
        checkStatement();
        return fetchSize;
    }


    final public int getResultSetConcurrency() throws SQLException {
        checkStatement();
        return rsConcurrency;
    }


    final public int getResultSetType() throws SQLException {
        checkStatement();
        return rsType;
    }


    final public void addBatch(String sql) {
        if (batches == null)
            batches = new ArrayList();
        batches.add(sql);
    }


    public void clearBatch() throws SQLException {
        checkStatement();
        if (batches == null)
            return;
        batches.clear();
    }


    public int[] executeBatch() throws BatchUpdateException {
        if (batches == null)
            return new int[0];
        final int[] result = new int[batches.size()];
        BatchUpdateException failed = null;
        for (int i = 0; i < result.length; i++) {
            try {
                result[i] = executeUpdate((String) batches.get(i));
            } catch (SQLException ex) {
                result[i] = EXECUTE_FAILED;
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


    final public Connection getConnection() {
        return con;
    }


    final public boolean getMoreResults(int current) throws SQLException {
        switch (current) {
            case CLOSE_ALL_RESULTS:
                // currently there exists only one ResultSet
            case CLOSE_CURRENT_RESULT:
                ResultSet rs = cmd.getResultSet();
                cmd.rs = null;
                if (rs != null)
                    rs.close();
                break;
            case KEEP_CURRENT_RESULT:
                break;
            default:
                throw SmallSQLException.create(Language.FLAGVALUE_INVALID, String.valueOf(current));
        }
        return cmd.getMoreResults();
    }


    final void setNeedGeneratedKeys(int autoGeneratedKeys) throws SQLException {
        switch (autoGeneratedKeys) {
            case NO_GENERATED_KEYS:
                break;
            case RETURN_GENERATED_KEYS:
                needGeneratedKeys = true;
                break;
            default:
                throw SmallSQLException.create(Language.ARGUMENT_INVALID, String.valueOf(autoGeneratedKeys));
        }
    }


    final void setNeedGeneratedKeys(int[] columnIndexes) throws SQLException {
        needGeneratedKeys = columnIndexes != null;
        generatedKeyIndexes = columnIndexes;
    }


    final void setNeedGeneratedKeys(String[] columnNames) throws SQLException {
        needGeneratedKeys = columnNames != null;
        generatedKeyNames = columnNames;
    }


    final boolean needGeneratedKeys() {
        return needGeneratedKeys;
    }


    final int[] getGeneratedKeyIndexes() {
        return generatedKeyIndexes;
    }


    final String[] getGeneratedKeyNames() {
        return generatedKeyNames;
    }


    /**
     * Set on execution the result with the generated keys.
     *
     * @param rs
     */
    final void setGeneratedKeys(ResultSet rs) {
        generatedKeys = rs;
    }


    final public ResultSet getGeneratedKeys() throws SQLException {
        if (generatedKeys == null)
            throw SmallSQLException.create(Language.GENER_KEYS_UNREQUIRED);
        return generatedKeys;
    }


    final public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        setNeedGeneratedKeys(autoGeneratedKeys);
        return executeUpdate(sql);
    }


    final public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        setNeedGeneratedKeys(columnIndexes);
        return executeUpdate(sql);
    }


    final public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        setNeedGeneratedKeys(columnNames);
        return executeUpdate(sql);
    }


    final public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        setNeedGeneratedKeys(autoGeneratedKeys);
        return execute(sql);
    }


    final public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        setNeedGeneratedKeys(columnIndexes);
        return execute(sql);
    }


    final public boolean execute(String sql, String[] columnNames) throws SQLException {
        setNeedGeneratedKeys(columnNames);
        return execute(sql);
    }


    final public int getResultSetHoldability() throws SQLException {
        /** @todo: Implement this java.sql.Statement method */
        throw util.generateUnsupportedOperation();
    }

    /**
     * Retrieves whether this <code>Statement</code> object has been closed. A <code>Statement</code> is closed if the
     * method close has been called on it, or if it is automatically closed.
     *
     * @return true if this <code>Statement</code> object is closed; false if it is still open
     */
    @Override
    public boolean isClosed() {
        return this.isClosed;
    }

    /**
     * Requests that a <code>Statement</code> be pooled or not pooled.  The value
     * specified is a hint to the statement pool implementation indicating
     * whether the application wants the statement to be pooled.  It is up to
     * the statement pool manager as to whether the hint is used.
     * <p>
     * The poolable value of a statement is applicable to both internal
     * statement caches implemented by the driver and external statement caches
     * implemented by application servers and other applications.
     * <p>
     * By default, a <code>Statement</code> is not poolable when created, and
     * a <code>PreparedStatement</code> and <code>CallableStatement</code>
     * are poolable when created.
     *
     * @param poolable requests that the statement be pooled if true and
     *                 that the statement not be pooled if false
     * @throws SQLException if this method is called on a closed
     *                      <code>Statement</code>
     */
    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        throw util.generateUnsupportedOperation();
    }

    /**
     * Returns a  value indicating whether the <code>Statement</code>
     * is poolable or not.
     *
     * @return <code>true</code> if the <code>Statement</code>
     * is poolable; <code>false</code> otherwise
     * @throws SQLException if this method is called on a closed
     *                      <code>Statement</code>
     * @see Statement#setPoolable(boolean) setPoolable(boolean)
     * @since 1.6
     */
    @Override
    public boolean isPoolable() throws SQLException {
        throw util.generateUnsupportedOperation();
    }

    /**
     * Specifies that this {@code Statement} will be closed when all its
     * dependent result sets are closed. If execution of the {@code Statement}
     * does not produce any result sets, this method has no effect.
     * <p>
     * <strong>Note:</strong> Multiple calls to {@code closeOnCompletion} do
     * not toggle the effect on this {@code Statement}. However, a call to
     * {@code closeOnCompletion} does effect both the subsequent execution of
     * statements, and statements that currently have open, dependent,
     * result sets.
     *
     * @throws SQLException if this method is called on a closed
     *                      {@code Statement}
     * @since 1.7
     */
    @Override
    public void closeOnCompletion() throws SQLException {
        throw util.generateUnsupportedOperation();
    }

    /**
     * Returns a value indicating whether this {@code Statement} will be
     * closed when all its dependent result sets are closed.
     *
     * @return {@code true} if the {@code Statement} will be closed when all
     * of its dependent result sets are closed; {@code false} otherwise
     * @throws SQLException if this method is called on a closed
     *                      {@code Statement}
     * @since 1.7
     */
    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        throw util.generateUnsupportedOperation();
    }


    void checkStatement() throws SQLException {
        if (isClosed) {
            throw SmallSQLException.create(Language.STMT_IS_CLOSED);
        }
    }

    /**
     * Returns an object that implements the given interface to allow access to
     * non-standard methods, or standard methods not exposed by the proxy.
     * <p>
     * If the receiver implements the interface then the result is the receiver
     * or a proxy for the receiver. If the receiver is a wrapper
     * and the wrapped object implements the interface then the result is the
     * wrapped object or a proxy for the wrapped object. Otherwise return the
     * the result of calling <code>unwrap</code> recursively on the wrapped object
     * or a proxy for that result. If the receiver is not a
     * wrapper and does not implement the interface, then an <code>SQLException</code> is thrown.
     *
     * @param iface A Class defining an interface that the result must implement.
     * @return an object that implements the interface. May be a proxy for the actual implementing object.
     * @throws SQLException If no object found that implements the interface
     * @since 1.6
     */
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw util.generateUnsupportedOperation();
    }

    /**
     * Returns true if this either implements the interface argument or is directly or indirectly a wrapper
     * for an object that does. Returns false otherwise. If this implements the interface then return true,
     * else if this is a wrapper then return the result of recursively calling <code>isWrapperFor</code> on the wrapped
     * object. If this does not implement the interface and is not a wrapper, return false.
     * This method should be implemented as a low-cost operation compared to <code>unwrap</code> so that
     * callers can use this method to avoid expensive <code>unwrap</code> calls that may fail. If this method
     * returns true then calling <code>unwrap</code> with the same argument should succeed.
     *
     * @param iface a Class defining an interface.
     * @return true if this implements the interface or directly or indirectly wraps an object that does.
     * @throws SQLException if an error occurs while determining whether this is a wrapper
     *                      for an object with the given interface.
     * @since 1.6
     */
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw util.generateUnsupportedOperation();
    }
}
