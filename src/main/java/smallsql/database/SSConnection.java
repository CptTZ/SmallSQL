/* =============================================================
 * SmallSQL : a free Java DBMS library for the Java(tm) platform
 * =============================================================
 *
 * (C) Copyright 2004-2011, by Volker Berlin.
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
 * SSConnection.java
 * ---------------
 * Author: Volker Berlin
 *
 */
package smallsql.database;

import smallsql.database.language.Language;
import smallsql.tools.util;

import java.nio.channels.FileChannel;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class SSConnection implements Connection {

    private final boolean readonly;
    private Database database;
    private boolean autoCommit = true;
    int isolationLevel = TRANSACTION_READ_COMMITTED; // see also getDefaultTransactionIsolation
    private List commitPages = new ArrayList();
    /**
     * The time on which a transaction is starting.
     */
    private long transactionTime;
    private final SSDatabaseMetaData metadata;
    private int holdability;
    final Logger log;

    SSConnection(Properties props) throws SQLException {
        SmallSQLException.setLanguage(props.get("locale"));
        log = new Logger();
        String name = props.getProperty("dbpath");
        readonly = "true".equals(props.getProperty("readonly"));
        boolean create = "true".equals(props.getProperty("create"));
        database = Database.getDatabase(name, this, create);
        metadata = new SSDatabaseMetaData(this);
    }

    /**
     * Create a copy of the Connection with it own transaction room.
     *
     * @param con the original Connection
     */
    SSConnection(SSConnection con) {
        readonly = con.readonly;
        database = con.database;
        metadata = con.metadata;
        log = con.log;
    }

    /**
     * @param returnNull If null is a valid return value for the case of not connected to a database.
     * @throws SQLException If not connected and returnNull is false.
     */
    Database getDatabase(boolean returnNull) throws SQLException {
        testClosedConnection();
        if (!returnNull && database == null) throw SmallSQLException.create(Language.DB_NOTCONNECTED);
        return database;
    }

    /**
     * Get a monitor object for all synchronized blocks on connection base. Multiple calls return the same object.
     *
     * @return a unique object of this connection
     */
    Object getMonitor() {
        return this;
    }

    public Statement createStatement() throws SQLException {
        return new SSStatement(this);
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return new SSPreparedStatement(this, sql);
    }

    public CallableStatement prepareCall(String sql) throws SQLException {
        return new SSCallableStatement(this, sql);
    }


    public String nativeSQL(String sql) {
        return sql;
    }


    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if (log.isLogging()) log.println("AutoCommit:" + autoCommit);
        if (this.autoCommit != autoCommit) {
            commit();
            this.autoCommit = autoCommit;
        }
    }


    public boolean getAutoCommit() {
        return autoCommit;
    }


    /**
     * Add a page for later commit or rollback.
     */
    void add(TransactionStep storePage) throws SQLException {
        testClosedConnection();
        synchronized (getMonitor()) {
            commitPages.add(storePage);
        }
    }


    public void commit() throws SQLException {
        log.println("Commit");
        testClosedConnection();
        synchronized (getMonitor()) {
            try {
                int count = commitPages.size();
                for (int i = 0; i < count; i++) {
                    TransactionStep page = (TransactionStep) commitPages.get(i);
                    page.commit();
                }
                for (int i = 0; i < count; i++) {
                    TransactionStep page = (TransactionStep) commitPages.get(i);
                    page.freeLock();
                }
                commitPages.clear();
                transactionTime = System.currentTimeMillis();
            } catch (Throwable e) {
                rollback();
                throw SmallSQLException.createFromException(e);
            }
        }
    }


    /**
     * Discard all changes of a file because it was deleted.
     */
    void rollbackFile(FileChannel raFile) throws SQLException {
        testClosedConnection();
        // remove the all commits that point to this table
        synchronized (getMonitor()) {
            for (int i = commitPages.size() - 1; i >= 0; i--) {
                TransactionStep page = (TransactionStep) commitPages.get(i);
                if (page.raFile == raFile) {
                    page.rollback();
                    page.freeLock();
                }
            }
        }
    }


    void rollback(int savepoint) throws SQLException {
        testClosedConnection();
        synchronized (getMonitor()) {
            for (int i = commitPages.size() - 1; i >= savepoint; i--) {
                TransactionStep page = (TransactionStep) commitPages.remove(i);
                page.rollback();
                page.freeLock();
            }
        }
    }


    public void rollback() throws SQLException {
        log.println("Rollback");
        testClosedConnection();
        synchronized (getMonitor()) {
            int count = commitPages.size();
            for (int i = 0; i < count; i++) {
                TransactionStep page = (TransactionStep) commitPages.get(i);
                page.rollback();
                page.freeLock();
            }
            commitPages.clear();
            transactionTime = System.currentTimeMillis();
        }
    }


    public void close() throws SQLException {
        rollback();
        database = null;
        commitPages = null;
        Database.closeConnection(this);
    }

    /**
     * Test if the connection was closed. for example from another thread.
     *
     * @throws SQLException if the connection was closed.
     */
    final void testClosedConnection() throws SQLException {
        if (isClosed()) throw SmallSQLException.create(Language.CONNECTION_CLOSED);
    }

    public boolean isClosed() {
        return (commitPages == null);
    }


    public DatabaseMetaData getMetaData() {
        return metadata;
    }


    public void setReadOnly(boolean readOnly) {
        //TODO Connection ReadOnly implementing
    }


    public boolean isReadOnly() {
        return readonly;
    }


    public void setCatalog(String catalog) throws SQLException {
        testClosedConnection();
        database = Database.getDatabase(catalog, this, false);
    }


    public String getCatalog() {
        if (database == null)
            return "";
        return database.getName();
    }


    public void setTransactionIsolation(int level) throws SQLException {
        if (!metadata.supportsTransactionIsolationLevel(level)) {
            throw SmallSQLException.create(Language.ISOLATION_UNKNOWN, String.valueOf(level));
        }
        isolationLevel = level;
    }


    public int getTransactionIsolation() {
        return isolationLevel;
    }


    public SQLWarning getWarnings() {
        return null;
    }


    public void clearWarnings() {
        //TODO support for Warnings
    }


    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return new SSStatement(this, resultSetType, resultSetConcurrency);
    }


    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return new SSPreparedStatement(this, sql, resultSetType, resultSetConcurrency);
    }


    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return new SSCallableStatement(this, sql, resultSetType, resultSetConcurrency);
    }


    public Map getTypeMap() {
        return null;
    }


    public void setTypeMap(Map map) {
        //TODO support for TypeMap
    }


    public void setHoldability(int holdability) {
        this.holdability = holdability;
    }


    public int getHoldability() {
        return holdability;
    }


    int getSavepoint() throws SQLException {
        testClosedConnection();
        return commitPages.size(); // the call is atomic, that it need not be synchronized
    }


    public Savepoint setSavepoint() throws SQLException {
        return new SSSavepoint(getSavepoint(), null, transactionTime);
    }


    public Savepoint setSavepoint(String name) throws SQLException {
        return new SSSavepoint(getSavepoint(), name, transactionTime);
    }


    public void rollback(Savepoint savepoint) throws SQLException {
        if (savepoint instanceof SSSavepoint) {
            if (((SSSavepoint) savepoint).transactionTime != transactionTime) {
                throw SmallSQLException.create(Language.SAVEPT_INVALID_TRANS);
            }
            rollback(savepoint.getSavepointId());
            return;
        }
        throw SmallSQLException.create(Language.SAVEPT_INVALID_DRIVER, savepoint);
    }


    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        if (savepoint instanceof SSSavepoint) {
            ((SSSavepoint) savepoint).transactionTime = 0;
            return;
        }
        throw SmallSQLException.create(Language.SAVEPT_INVALID_DRIVER, new Object[]{savepoint});
    }


    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        //TODO resultSetHoldability
        return new SSStatement(this, resultSetType, resultSetConcurrency);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        //TODO resultSetHoldability
        return new SSPreparedStatement(this, sql);
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        //TODO resultSetHoldability
        return new SSCallableStatement(this, sql, resultSetType, resultSetConcurrency);
    }


    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        SSPreparedStatement pr = new SSPreparedStatement(this, sql);
        pr.setNeedGeneratedKeys(autoGeneratedKeys);
        return pr;
    }


    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        SSPreparedStatement pr = new SSPreparedStatement(this, sql);
        pr.setNeedGeneratedKeys(columnIndexes);
        return pr;
    }


    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        SSPreparedStatement pr = new SSPreparedStatement(this, sql);
        pr.setNeedGeneratedKeys(columnNames);
        return pr;
    }

    /**
     * Constructs an object that implements the <code>Clob</code> interface. The object
     * returned initially contains no data.  The <code>setAsciiStream</code>,
     * <code>setCharacterStream</code> and <code>setString</code> methods of
     * the <code>Clob</code> interface may be used to add data to the <code>Clob</code>.
     *
     * @return An object that implements the <code>Clob</code> interface
     * @throws SQLException                    if an object that implements the
     *                                         <code>Clob</code> interface can not be constructed, this method is
     *                                         called on a closed connection or a database access error occurs.
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this data type
     * @since 1.6
     */
    @Override
    public Clob createClob() throws SQLException {
        throw util.generateUnsupportedOperation();
    }

    /**
     * Constructs an object that implements the <code>Blob</code> interface. The object
     * returned initially contains no data.  The <code>setBinaryStream</code> and
     * <code>setBytes</code> methods of the <code>Blob</code> interface may be used to add data to
     * the <code>Blob</code>.
     *
     * @return An object that implements the <code>Blob</code> interface
     * @throws SQLException                    if an object that implements the
     *                                         <code>Blob</code> interface can not be constructed, this method is
     *                                         called on a closed connection or a database access error occurs.
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this data type
     * @since 1.6
     */
    @Override
    public Blob createBlob() throws SQLException {
        throw util.generateUnsupportedOperation();
    }

    /**
     * Constructs an object that implements the <code>NClob</code> interface. The object
     * returned initially contains no data.  The <code>setAsciiStream</code>,
     * <code>setCharacterStream</code> and <code>setString</code> methods of the <code>NClob</code> interface may
     * be used to add data to the <code>NClob</code>.
     *
     * @return An object that implements the <code>NClob</code> interface
     * @throws SQLException                    if an object that implements the
     *                                         <code>NClob</code> interface can not be constructed, this method is
     *                                         called on a closed connection or a database access error occurs.
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this data type
     * @since 1.6
     */
    @Override
    public NClob createNClob() throws SQLException {
        throw util.generateUnsupportedOperation();
    }

    /**
     * Constructs an object that implements the <code>SQLXML</code> interface. The object
     * returned initially contains no data. The <code>createXmlStreamWriter</code> object and
     * <code>setString</code> method of the <code>SQLXML</code> interface may be used to add data to the <code>SQLXML</code>
     * object.
     *
     * @return An object that implements the <code>SQLXML</code> interface
     * @throws SQLException                    if an object that implements the <code>SQLXML</code> interface can not
     *                                         be constructed, this method is
     *                                         called on a closed connection or a database access error occurs.
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this data type
     * @since 1.6
     */
    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw util.generateUnsupportedOperation();
    }

    /**
     * Returns true if the connection has not been closed and is still valid.
     * The driver shall submit a query on the connection or use some other
     * mechanism that positively verifies the connection is still valid when
     * this method is called.
     * <p>
     * The query submitted by the driver to validate the connection shall be
     * executed in the context of the current transaction.
     *
     * @param timeout -             The time in seconds to wait for the database operation
     *                used to validate the connection to complete.  If
     *                the timeout period expires before the operation
     *                completes, this method returns false.  A value of
     *                0 indicates a timeout is not applied to the
     *                database operation.
     * @return true if the connection is valid, false otherwise
     * @throws SQLException if the value supplied for <code>timeout</code>
     *                      is less than 0
     * @see DatabaseMetaData#getClientInfoProperties
     * @since 1.6
     */
    @Override
    public boolean isValid(int timeout) throws SQLException {
        throw util.generateUnsupportedOperation();
    }

    /**
     * Sets the value of the client info property specified by name to the
     * value specified by value.
     * <p>
     * Applications may use the <code>DatabaseMetaData.getClientInfoProperties</code>
     * method to determine the client info properties supported by the driver
     * and the maximum length that may be specified for each property.
     * <p>
     * The driver stores the value specified in a suitable location in the
     * database.  For example in a special register, session parameter, or
     * system table column.  For efficiency the driver may defer setting the
     * value in the database until the next time a statement is executed or
     * prepared.  Other than storing the client information in the appropriate
     * place in the database, these methods shall not alter the behavior of
     * the connection in anyway.  The values supplied to these methods are
     * used for accounting, diagnostics and debugging purposes only.
     * <p>
     * The driver shall generate a warning if the client info name specified
     * is not recognized by the driver.
     * <p>
     * If the value specified to this method is greater than the maximum
     * length for the property the driver may either truncate the value and
     * generate a warning or generate a <code>SQLClientInfoException</code>.  If the driver
     * generates a <code>SQLClientInfoException</code>, the value specified was not set on the
     * connection.
     * <p>
     * The following are standard client info properties.  Drivers are not
     * required to support these properties however if the driver supports a
     * client info property that can be described by one of the standard
     * properties, the standard property name should be used.
     *
     * <ul>
     * <li>ApplicationName  -       The name of the application currently utilizing
     * the connection</li>
     * <li>ClientUser               -       The name of the user that the application using
     * the connection is performing work for.  This may
     * not be the same as the user name that was used
     * in establishing the connection.</li>
     * <li>ClientHostname   -       The hostname of the computer the application
     * using the connection is running on.</li>
     * </ul>
     *
     * @param name  The name of the client info property to set
     * @param value The value to set the client info property to.  If the
     *              value is null, the current value of the specified
     *              property is cleared.
     * @throws SQLClientInfoException if the database server returns an error while
     *                                setting the client info value on the database server or this method
     *                                is called on a closed connection
     * @since 1.6
     */
    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw util.generateUnsupportedOperation();
    }

    /**
     * Sets the value of the connection's client info properties.  The
     * <code>Properties</code> object contains the names and values of the client info
     * properties to be set.  The set of client info properties contained in
     * the properties list replaces the current set of client info properties
     * on the connection.  If a property that is currently set on the
     * connection is not present in the properties list, that property is
     * cleared.  Specifying an empty properties list will clear all of the
     * properties on the connection.  See <code>setClientInfo (String, String)</code> for
     * more information.
     * <p>
     * If an error occurs in setting any of the client info properties, a
     * <code>SQLClientInfoException</code> is thrown. The <code>SQLClientInfoException</code>
     * contains information indicating which client info properties were not set.
     * The state of the client information is unknown because
     * some databases do not allow multiple client info properties to be set
     * atomically.  For those databases, one or more properties may have been
     * set before the error occurred.
     *
     * @param properties the list of client info properties to set
     * @throws SQLClientInfoException if the database server returns an error while
     *                                setting the clientInfo values on the database server or this method
     *                                is called on a closed connection
     * @see Connection#setClientInfo(String, String) setClientInfo(String, String)
     * @since 1.6
     */
    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw util.generateUnsupportedOperation();
    }

    /**
     * Returns the value of the client info property specified by name.  This
     * method may return null if the specified client info property has not
     * been set and does not have a default value.  This method will also
     * return null if the specified client info property name is not supported
     * by the driver.
     * <p>
     * Applications may use the <code>DatabaseMetaData.getClientInfoProperties</code>
     * method to determine the client info properties supported by the driver.
     *
     * @param name The name of the client info property to retrieve
     * @return The value of the client info property specified
     * @throws SQLException if the database server returns an error when
     *                      fetching the client info value from the database
     *                      or this method is called on a closed connection
     * @see DatabaseMetaData#getClientInfoProperties
     * @since 1.6
     */
    @Override
    public String getClientInfo(String name) throws SQLException {
        throw util.generateUnsupportedOperation();
    }

    /**
     * Returns a list containing the name and current value of each client info
     * property supported by the driver.  The value of a client info property
     * may be null if the property has not been set and does not have a
     * default value.
     *
     * @return A <code>Properties</code> object that contains the name and current value of
     * each of the client info properties supported by the driver.
     * @throws SQLException if the database server returns an error when
     *                      fetching the client info values from the database
     *                      or this method is called on a closed connection
     * @since 1.6
     */
    @Override
    public Properties getClientInfo() throws SQLException {
        throw util.generateUnsupportedOperation();
    }

    /**
     * Factory method for creating Array objects.
     * <p>
     * <b>Note: </b>When <code>createArrayOf</code> is used to create an array object
     * that maps to a primitive data type, then it is implementation-defined
     * whether the <code>Array</code> object is an array of that primitive
     * data type or an array of <code>Object</code>.
     * <p>
     * <b>Note: </b>The JDBC driver is responsible for mapping the elements
     * <code>Object</code> array to the default JDBC SQL type defined in
     * java.sql.Types for the given class of <code>Object</code>. The default
     * mapping is specified in Appendix B of the JDBC specification.  If the
     * resulting JDBC type is not the appropriate type for the given typeName then
     * it is implementation defined whether an <code>SQLException</code> is
     * thrown or the driver supports the resulting conversion.
     *
     * @param typeName the SQL name of the type the elements of the array map to. The typeName is a
     *                 database-specific name which may be the name of a built-in type, a user-defined type or a standard  SQL type supported by this database. This
     *                 is the value returned by <code>Array.getBaseTypeName</code>
     * @param elements the elements that populate the returned object
     * @return an Array object whose elements map to the specified SQL type
     * @throws SQLException                    if a database error occurs, the JDBC type is not
     *                                         appropriate for the typeName and the conversion is not supported, the typeName is null or this method is called on a closed connection
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support this data type
     * @since 1.6
     */
    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw util.generateUnsupportedOperation();
    }

    /**
     * Factory method for creating Struct objects.
     *
     * @param typeName   the SQL type name of the SQL structured type that this <code>Struct</code>
     *                   object maps to. The typeName is the name of  a user-defined type that
     *                   has been defined for this database. It is the value returned by
     *                   <code>Struct.getSQLTypeName</code>.
     * @param attributes the attributes that populate the returned object
     * @return a Struct object that maps to the given SQL type and is populated with the given attributes
     * @throws SQLException                    if a database error occurs, the typeName is null or this method is called on a closed connection
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support this data type
     * @since 1.6
     */
    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw util.generateUnsupportedOperation();
    }

    /**
     * Sets the given schema name to access.
     * <p>
     * If the driver does not support schemas, it will
     * silently ignore this request.
     * <p>
     * Calling {@code setSchema} has no effect on previously created or prepared
     * {@code Statement} objects. It is implementation defined whether a DBMS
     * prepare operation takes place immediately when the {@code Connection}
     * method {@code prepareStatement} or {@code prepareCall} is invoked.
     * For maximum portability, {@code setSchema} should be called before a
     * {@code Statement} is created or prepared.
     *
     * @param schema the name of a schema  in which to work
     * @throws SQLException if a database access error occurs
     *                      or this method is called on a closed connection
     * @see #getSchema
     * @since 1.7
     */
    @Override
    public void setSchema(String schema) throws SQLException {
        throw util.generateUnsupportedOperation();
    }

    /**
     * Retrieves this <code>Connection</code> object's current schema name.
     *
     * @return the current schema name or <code>null</code> if there is none
     * @throws SQLException if a database access error occurs
     *                      or this method is called on a closed connection
     * @see #setSchema
     * @since 1.7
     */
    @Override
    public String getSchema() throws SQLException {
        throw util.generateUnsupportedOperation();
    }

    /**
     * Terminates an open connection.  Calling <code>abort</code> results in:
     * <ul>
     * <li>The connection marked as closed
     * <li>Closes any physical connection to the database
     * <li>Releases resources used by the connection
     * <li>Insures that any thread that is currently accessing the connection
     * will either progress to completion or throw an <code>SQLException</code>.
     * </ul>
     * <p>
     * Calling <code>abort</code> marks the connection closed and releases any
     * resources. Calling <code>abort</code> on a closed connection is a
     * no-op.
     * <p>
     * It is possible that the aborting and releasing of the resources that are
     * held by the connection can take an extended period of time.  When the
     * <code>abort</code> method returns, the connection will have been marked as
     * closed and the <code>Executor</code> that was passed as a parameter to abort
     * may still be executing tasks to release resources.
     * <p>
     * This method checks to see that there is an <code>SQLPermission</code>
     * object before allowing the method to proceed.  If a
     * <code>SecurityManager</code> exists and its
     * <code>checkPermission</code> method denies calling <code>abort</code>,
     * this method throws a
     * <code>java.lang.SecurityException</code>.
     *
     * @param executor The <code>Executor</code>  implementation which will
     *                 be used by <code>abort</code>.
     * @throws SQLException      if a database access error occurs or
     *                           the {@code executor} is {@code null},
     * @throws SecurityException if a security manager exists and its
     *                           <code>checkPermission</code> method denies calling <code>abort</code>
     * @see SecurityManager#checkPermission
     * @see Executor
     * @since 1.7
     */
    @Override
    public void abort(Executor executor) throws SQLException {
        throw util.generateUnsupportedOperation();
    }

    /**
     * Sets the maximum period a <code>Connection</code> or
     * objects created from the <code>Connection</code>
     * will wait for the database to reply to any one request. If any
     * request remains unanswered, the waiting method will
     * return with a <code>SQLException</code>, and the <code>Connection</code>
     * or objects created from the <code>Connection</code>  will be marked as
     * closed. Any subsequent use of
     * the objects, with the exception of the <code>close</code>,
     * <code>isClosed</code> or <code>Connection.isValid</code>
     * methods, will result in  a <code>SQLException</code>.
     * <p>
     * <b>Note</b>: This method is intended to address a rare but serious
     * condition where network partitions can cause threads issuing JDBC calls
     * to hang uninterruptedly in socket reads, until the OS TCP-TIMEOUT
     * (typically 10 minutes). This method is related to the
     * {@link #abort abort() } method which provides an administrator
     * thread a means to free any such threads in cases where the
     * JDBC connection is accessible to the administrator thread.
     * The <code>setNetworkTimeout</code> method will cover cases where
     * there is no administrator thread, or it has no access to the
     * connection. This method is severe in it's effects, and should be
     * given a high enough value so it is never triggered before any more
     * normal timeouts, such as transaction timeouts.
     * <p>
     * JDBC driver implementations  may also choose to support the
     * {@code setNetworkTimeout} method to impose a limit on database
     * response time, in environments where no network is present.
     * <p>
     * Drivers may internally implement some or all of their API calls with
     * multiple internal driver-database transmissions, and it is left to the
     * driver implementation to determine whether the limit will be
     * applied always to the response to the API call, or to any
     * single  request made during the API call.
     * <p>
     * <p>
     * This method can be invoked more than once, such as to set a limit for an
     * area of JDBC code, and to reset to the default on exit from this area.
     * Invocation of this method has no impact on already outstanding
     * requests.
     * <p>
     * The {@code Statement.setQueryTimeout()} timeout value is independent of the
     * timeout value specified in {@code setNetworkTimeout}. If the query timeout
     * expires  before the network timeout then the
     * statement execution will be canceled. If the network is still
     * active the result will be that both the statement and connection
     * are still usable. However if the network timeout expires before
     * the query timeout or if the statement timeout fails due to network
     * problems, the connection will be marked as closed, any resources held by
     * the connection will be released and both the connection and
     * statement will be unusable.
     * <p>
     * When the driver determines that the {@code setNetworkTimeout} timeout
     * value has expired, the JDBC driver marks the connection
     * closed and releases any resources held by the connection.
     * <p>
     * <p>
     * This method checks to see that there is an <code>SQLPermission</code>
     * object before allowing the method to proceed.  If a
     * <code>SecurityManager</code> exists and its
     * <code>checkPermission</code> method denies calling
     * <code>setNetworkTimeout</code>, this method throws a
     * <code>java.lang.SecurityException</code>.
     *
     * @param executor     The <code>Executor</code>  implementation which will
     *                     be used by <code>setNetworkTimeout</code>.
     * @param milliseconds The time in milliseconds to wait for the database
     *                     operation
     *                     to complete.  If the JDBC driver does not support milliseconds, the
     *                     JDBC driver will round the value up to the nearest second.  If the
     *                     timeout period expires before the operation
     *                     completes, a SQLException will be thrown.
     *                     A value of 0 indicates that there is not timeout for database operations.
     * @throws SQLException                    if a database access error occurs, this
     *                                         method is called on a closed connection,
     *                                         the {@code executor} is {@code null},
     *                                         or the value specified for <code>seconds</code> is less than 0.
     * @throws SecurityException               if a security manager exists and its
     *                                         <code>checkPermission</code> method denies calling
     *                                         <code>setNetworkTimeout</code>.
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @see SecurityManager#checkPermission
     * @see Statement#setQueryTimeout
     * @see #getNetworkTimeout
     * @see #abort
     * @see Executor
     * @since 1.7
     */
    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw util.generateUnsupportedOperation();
    }

    /**
     * Retrieves the number of milliseconds the driver will
     * wait for a database request to complete.
     * If the limit is exceeded, a
     * <code>SQLException</code> is thrown.
     *
     * @return the current timeout limit in milliseconds; zero means there is
     * no limit
     * @throws SQLException                    if a database access error occurs or
     *                                         this method is called on a closed <code>Connection</code>
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @see #setNetworkTimeout
     * @since 1.7
     */
    @Override
    public int getNetworkTimeout() throws SQLException {
        throw util.generateUnsupportedOperation();
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
