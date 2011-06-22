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

import java.nio.channels.FileChannel;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import smallsql.database.language.Language;

public class SSConnection implements Connection {

    private final boolean readonly;
    private Database database;
    private boolean autoCommit = true;
    int isolationLevel = TRANSACTION_READ_COMMITTED; // see also getDefaultTransactionIsolation
    private List commitPages = new ArrayList();
    /** The time on which a transaction is starting. */
    private long transactionTime;
    private final SSDatabaseMetaData metadata;
    private int holdability;
    final Logger log;

    SSConnection( Properties props ) throws SQLException{
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
     * @param con the original Connection
     */
    SSConnection( SSConnection con ){
        readonly = con.readonly;
        database = con.database;
        metadata = con.metadata;
        log      = con.log;
    }
    
    /**
     * @param returnNull If null is a valid return value for the case of not connected to a database.
     * @throws SQLException If not connected and returnNull is false.
     */
    Database getDatabase(boolean returnNull) throws SQLException{
        testClosedConnection();
    	if(!returnNull && database == null) throw SmallSQLException.create(Language.DB_NOTCONNECTED);
    	return database;
    }

    /**
     * Get a monitor object for all synchronized blocks on connection base. Multiple calls return the same object.
     * 
     * @return a unique object of this connection
     */
    Object getMonitor(){
        return this;
    }
    
    public Statement createStatement() throws SQLException {
        return new SSStatement(this);
    }
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return new SSPreparedStatement( this, sql);
    }
    public CallableStatement prepareCall(String sql) throws SQLException {
        return new SSCallableStatement( this, sql);
    }
    
    
    public String nativeSQL(String sql){
        return sql;
    }
    
    
    public void setAutoCommit(boolean autoCommit) throws SQLException {
		if(log.isLogging()) log.println("AutoCommit:"+autoCommit);
    	if(this.autoCommit != autoCommit){
    		commit();
    		this.autoCommit = autoCommit;
    	}
    }
    
    
    public boolean getAutoCommit(){
        return autoCommit;
    }
    
    
	/**
	 * Add a page for later commit or rollback. 
	 */
	void add(TransactionStep storePage) throws SQLException{
		testClosedConnection();
		synchronized(getMonitor()){
            commitPages.add(storePage);
        }
	}
	
	
    public void commit() throws SQLException {
        log.println("Commit");
        testClosedConnection();
        synchronized(getMonitor()){
    	try{
	            int count = commitPages.size();
	            for(int i=0; i<count; i++){
	                TransactionStep page = (TransactionStep)commitPages.get(i);
	                page.commit();
	            }
				for(int i=0; i<count; i++){
				    TransactionStep page = (TransactionStep)commitPages.get(i);
					page.freeLock();
				}
	            commitPages.clear();
	            transactionTime = System.currentTimeMillis();
    	}catch(Throwable e){
    		rollback();
    		throw SmallSQLException.createFromException(e);
    	}
        }
    }
    
	
	/**
	 * Discard all changes of a file because it was deleted.
	 */
	void rollbackFile(FileChannel raFile) throws SQLException{
		testClosedConnection();
		// remove the all commits that point to this table
		synchronized(getMonitor()){
            for(int i = commitPages.size() - 1; i >= 0; i--){
                TransactionStep page = (TransactionStep)commitPages.get(i);
                if(page.raFile == raFile){
                    page.rollback();
                    page.freeLock();
                }
            }
        }
	}
	
    
    void rollback(int savepoint) throws SQLException{
		testClosedConnection();
		synchronized(getMonitor()){
            for(int i = commitPages.size() - 1; i >= savepoint; i--){
                TransactionStep page = (TransactionStep)commitPages.remove(i);
                page.rollback();
                page.freeLock();
            }
        }
    }
    
    
    public void rollback() throws SQLException {
		log.println("Rollback");
		testClosedConnection();
        synchronized(getMonitor()){
            int count = commitPages.size();
            for(int i=0; i<count; i++){
                TransactionStep page = (TransactionStep)commitPages.get(i);
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
     * @throws SQLException
     *             if the connection was closed.
     */
	final void testClosedConnection() throws SQLException{
		if(isClosed()) throw SmallSQLException.create(Language.CONNECTION_CLOSED);
	}
    
    public boolean isClosed(){
        return (commitPages == null);
    }
    
    
    public DatabaseMetaData getMetaData(){
        return metadata;
    }
    
    
    public void setReadOnly(boolean readOnly){
        //TODO Connection ReadOnly implementing
    }
    
    
    public boolean isReadOnly(){
        return readonly;
    }
    
    
    public void setCatalog(String catalog) throws SQLException {
        testClosedConnection();
        database = Database.getDatabase(catalog, this, false);
    }
    
    
    public String getCatalog(){
    	if(database == null)
    		return "";
        return database.getName();
    }
    
    
    public void setTransactionIsolation(int level) throws SQLException {
    	if(!metadata.supportsTransactionIsolationLevel(level)) {
    		throw SmallSQLException.create(Language.ISOLATION_UNKNOWN, String.valueOf(level));
    	}
        isolationLevel = level;        
    }
    
    
    public int getTransactionIsolation(){
        return isolationLevel;
    }
    
    
    public SQLWarning getWarnings(){
        return null;
    }
    
    
    public void clearWarnings(){
        //TODO support for Warnings
    }
    
    
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return new SSStatement( this, resultSetType, resultSetConcurrency);
    }
    
    
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return new SSPreparedStatement( this, sql, resultSetType, resultSetConcurrency);
    }
    
    
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return new SSCallableStatement( this, sql, resultSetType, resultSetConcurrency);
    }
    
    
    public Map getTypeMap(){
        return null;
    }
    
    
    public void setTypeMap(Map map){
        //TODO support for TypeMap
    }
    
    
    public void setHoldability(int holdability){
        this.holdability = holdability;
    }
    
    
    public int getHoldability(){
        return holdability;
    }
    
    
	int getSavepoint() throws SQLException{
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
    	if(savepoint instanceof SSSavepoint){
    		if(((SSSavepoint)savepoint).transactionTime != transactionTime){
				throw SmallSQLException.create(Language.SAVEPT_INVALID_TRANS);
    		}
    		rollback( savepoint.getSavepointId() );
    		return;
    	}
        throw SmallSQLException.create(Language.SAVEPT_INVALID_DRIVER, savepoint);
    }
    
    
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		if(savepoint instanceof SSSavepoint){
			((SSSavepoint)savepoint).transactionTime = 0;
			return;
		}
		throw SmallSQLException.create(Language.SAVEPT_INVALID_DRIVER, new Object[] { savepoint });
    }
    
    
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		//TODO resultSetHoldability
		return new SSStatement( this, resultSetType, resultSetConcurrency);
    }
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		//TODO resultSetHoldability
		return new SSPreparedStatement( this, sql);
    }
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    	//TODO resultSetHoldability
		return new SSCallableStatement( this, sql, resultSetType, resultSetConcurrency);
    }
    
    
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        SSPreparedStatement pr = new SSPreparedStatement( this, sql);
        pr.setNeedGeneratedKeys(autoGeneratedKeys);
        return pr;
    }
    
    
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        SSPreparedStatement pr = new SSPreparedStatement( this, sql);
        pr.setNeedGeneratedKeys(columnIndexes);
        return pr;
    }
    
    
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        SSPreparedStatement pr = new SSPreparedStatement( this, sql);
        pr.setNeedGeneratedKeys(columnNames);
        return pr;
    }
}