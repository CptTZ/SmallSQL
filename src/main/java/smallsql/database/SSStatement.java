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

import java.sql.*;
import java.util.ArrayList;
import smallsql.database.language.Language;

class SSStatement implements Statement{

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


    SSStatement(SSConnection con) throws SQLException{
        this(con, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }


    SSStatement(SSConnection con, int rsType, int rsConcurrency) throws SQLException{
        this.con = con;
        this.rsType = rsType;
        this.rsConcurrency = rsConcurrency;
        con.testClosedConnection();
    }


    final public ResultSet executeQuery(String sql) throws SQLException{
        executeImpl(sql);
        return cmd.getQueryResult();
    }


    final public int executeUpdate(String sql) throws SQLException{
        executeImpl(sql);
        return cmd.getUpdateCount();
    }


    final public boolean execute(String sql) throws SQLException{
        executeImpl(sql);
        return cmd.getResultSet() != null;
    }


    final private void executeImpl(String sql) throws SQLException{
        checkStatement();
        generatedKeys = null;
        try{
            con.log.println(sql);
            SQLParser parser = new SQLParser();
            cmd = parser.parse(con, sql);
            if(maxRows != 0 && (cmd.getMaxRows() == -1 || cmd.getMaxRows() > maxRows))
                cmd.setMaxRows(maxRows);
            cmd.execute(con, this);
        }catch(Exception e){
            throw SmallSQLException.createFromException(e);
        }
        needGeneratedKeys = false;
        generatedKeyIndexes = null;
        generatedKeyNames = null;
    }


    final public void close(){
        con.log.println("Statement.close");
        isClosed = true;
        cmd = null;
        // TODO make Resources free;
    }


    final public int getMaxFieldSize(){
        return maxFieldSize;
    }


    final public void setMaxFieldSize(int max){
        maxFieldSize = max;
    }


    final public int getMaxRows(){
        return maxRows;
    }


    final public void setMaxRows(int max) throws SQLException{
        if(max < 0)
            throw SmallSQLException.create(Language.ROWS_WRONG_MAX, String.valueOf(max));
        maxRows = max;
    }


    final public void setEscapeProcessing(boolean enable) throws SQLException{
        checkStatement();
        // TODO enable/disable escape processing
    }


    final public int getQueryTimeout() throws SQLException{
        checkStatement();
        return queryTimeout;
    }


    final public void setQueryTimeout(int seconds) throws SQLException{
        checkStatement();
        queryTimeout = seconds;
    }


    final public void cancel() throws SQLException{
        checkStatement();
        // TODO Statement.cancel()
    }


    final public SQLWarning getWarnings(){
        return null;
    }


    final public void clearWarnings(){
        // TODO support for warnings
    }


    final public void setCursorName(String name) throws SQLException{
        /** @todo: Implement this java.sql.Statement.setCursorName method */
        throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION, "setCursorName");
    }


    final public ResultSet getResultSet() throws SQLException{
        checkStatement();
        return cmd.getResultSet();
    }


    final public int getUpdateCount() throws SQLException{
        checkStatement();
        return cmd.getUpdateCount();
    }


    final public boolean getMoreResults() throws SQLException{
        checkStatement();
        return getMoreResults(CLOSE_CURRENT_RESULT);
    }


    final public void setFetchDirection(int direction) throws SQLException{
        checkStatement();
        fetchDirection = direction;
    }


    final public int getFetchDirection() throws SQLException{
        checkStatement();
        return fetchDirection;
    }


    final public void setFetchSize(int rows) throws SQLException{
        checkStatement();
        fetchSize = rows;
    }


    final public int getFetchSize() throws SQLException{
        checkStatement();
        return fetchSize;
    }


    final public int getResultSetConcurrency() throws SQLException{
        checkStatement();
        return rsConcurrency;
    }


    final public int getResultSetType() throws SQLException{
        checkStatement();
        return rsType;
    }


    final public void addBatch(String sql){
        if(batches == null)
            batches = new ArrayList();
        batches.add(sql);
    }


    public void clearBatch() throws SQLException{
        checkStatement();
        if(batches == null)
            return;
        batches.clear();
    }


    public int[] executeBatch() throws BatchUpdateException{
        if(batches == null)
            return new int[0];
        final int[] result = new int[batches.size()];
        BatchUpdateException failed = null;
        for(int i = 0; i < result.length; i++){
            try{
                result[i] = executeUpdate((String)batches.get(i));
            }catch(SQLException ex){
                result[i] = EXECUTE_FAILED;
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


    final public Connection getConnection(){
        return con;
    }


    final public boolean getMoreResults(int current) throws SQLException{
        switch(current){
        case CLOSE_ALL_RESULTS:
        // currently there exists only one ResultSet
        case CLOSE_CURRENT_RESULT:
            ResultSet rs = cmd.getResultSet();
            cmd.rs = null;
            if(rs != null)
                rs.close();
            break;
        case KEEP_CURRENT_RESULT:
            break;
        default:
            throw SmallSQLException.create(Language.FLAGVALUE_INVALID, String.valueOf(current));
        }
        return cmd.getMoreResults();
    }


    final void setNeedGeneratedKeys(int autoGeneratedKeys) throws SQLException{
        switch(autoGeneratedKeys){
        case NO_GENERATED_KEYS:
            break;
        case RETURN_GENERATED_KEYS:
            needGeneratedKeys = true;
            break;
        default:
            throw SmallSQLException.create(Language.ARGUMENT_INVALID, String.valueOf(autoGeneratedKeys));
        }
    }


    final void setNeedGeneratedKeys(int[] columnIndexes) throws SQLException{
        needGeneratedKeys = columnIndexes != null;
        generatedKeyIndexes = columnIndexes;
    }


    final void setNeedGeneratedKeys(String[] columnNames) throws SQLException{
        needGeneratedKeys = columnNames != null;
        generatedKeyNames = columnNames;
    }


    final boolean needGeneratedKeys(){
        return needGeneratedKeys;
    }


    final int[] getGeneratedKeyIndexes(){
        return generatedKeyIndexes;
    }


    final String[] getGeneratedKeyNames(){
        return generatedKeyNames;
    }


    /**
     * Set on execution the result with the generated keys.
     * 
     * @param rs
     */
    final void setGeneratedKeys(ResultSet rs){
        generatedKeys = rs;
    }


    final public ResultSet getGeneratedKeys() throws SQLException{
        if(generatedKeys == null)
            throw SmallSQLException.create(Language.GENER_KEYS_UNREQUIRED);
        return generatedKeys;
    }


    final public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException{
        setNeedGeneratedKeys(autoGeneratedKeys);
        return executeUpdate(sql);
    }


    final public int executeUpdate(String sql, int[] columnIndexes) throws SQLException{
        setNeedGeneratedKeys(columnIndexes);
        return executeUpdate(sql);
    }


    final public int executeUpdate(String sql, String[] columnNames) throws SQLException{
        setNeedGeneratedKeys(columnNames);
        return executeUpdate(sql);
    }


    final public boolean execute(String sql, int autoGeneratedKeys) throws SQLException{
        setNeedGeneratedKeys(autoGeneratedKeys);
        return execute(sql);
    }


    final public boolean execute(String sql, int[] columnIndexes) throws SQLException{
        setNeedGeneratedKeys(columnIndexes);
        return execute(sql);
    }


    final public boolean execute(String sql, String[] columnNames) throws SQLException{
        setNeedGeneratedKeys(columnNames);
        return execute(sql);
    }


    final public int getResultSetHoldability() throws SQLException{
        /** @todo: Implement this java.sql.Statement method */
        throw new java.lang.UnsupportedOperationException("Method getResultSetHoldability() not yet implemented.");
    }


    void checkStatement() throws SQLException{
        if(isClosed){
            throw SmallSQLException.create(Language.STMT_IS_CLOSED);
        }
    }
}