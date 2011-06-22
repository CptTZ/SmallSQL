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
 * SSDatabaseMetaData.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.database;

import java.sql.*;
import java.util.ArrayList;


final class SSDatabaseMetaData implements DatabaseMetaData {
	final private SSConnection con;
	final private SSStatement st;
	
	
    /**
     * @throws SQLException Exception can be throw if the Connection already closed.
     */
    SSDatabaseMetaData(SSConnection con) throws SQLException{
		this.con = con;
		st = new SSStatement(con);
	}
	
    public boolean allProceduresAreCallable() {
    	return true;
    }
	
	
    public boolean allTablesAreSelectable() {
    	return true;
    }
	
	
    public String getURL() throws SQLException {
    	Database database = con.getDatabase(true);
    	if(database == null)
			return SSDriver.URL_PREFIX;
    	return SSDriver.URL_PREFIX + ':' + database.getName();
    }
	
	
    public String getUserName() {
    	return "";
    }
	
	
    public boolean isReadOnly() {
    	return false;
    }
	
	
    public boolean nullsAreSortedHigh() {
		return false;
    }
	
	
    public boolean nullsAreSortedLow() {
		return true;
    }
	
	
    public boolean nullsAreSortedAtStart() {
		return false;
    }
	
	
    public boolean nullsAreSortedAtEnd() {
		return false;
    }
	
	
    public String getDatabaseProductName() {
    	return "SmallSQL Database";
    }
	
	
    public String getDatabaseProductVersion() {
    	return getDriverVersion();
    }
	
	
    public String getDriverName(){
    	return "SmallSQL Driver";
    }
	
	
    public String getDriverVersion() {
    	return getDriverMajorVersion() + "." + SSDriver.drv.getMinorVersion();
    }
	
	
    public int getDriverMajorVersion() {
    	return SSDriver.drv.getMajorVersion();
    }
	
	
    public int getDriverMinorVersion() {
		return SSDriver.drv.getMinorVersion();
    }
	
	
    public boolean usesLocalFiles() {
    	return false;
    }
	
	
    public boolean usesLocalFilePerTable() {
    	return false;
    }
	
	
    public boolean supportsMixedCaseIdentifiers() {
    	return true;
    }
	
	
    public boolean storesUpperCaseIdentifiers() {
    	return false;
    }
	
	
    public boolean storesLowerCaseIdentifiers() {
    	return false;
    }
	
	
    public boolean storesMixedCaseIdentifiers() {
    	return true;
    }
	
	
    public boolean supportsMixedCaseQuotedIdentifiers() {
    	return true;
    }
	
	
    public boolean storesUpperCaseQuotedIdentifiers() {
    	return false;
    }
	
	
    public boolean storesLowerCaseQuotedIdentifiers() {
    	return false;
    }
	
	
    public boolean storesMixedCaseQuotedIdentifiers() {
    	return true;
    }
	
	
    public String getIdentifierQuoteString() {
    	return "\"";
    }
	
	
    public String getSQLKeywords() {
    	return "database,use";
    }
    
    
    private String getFunctions(int from, int to){
		StringBuffer buf = new StringBuffer();
		for(int i=from; i<=to; i++){
			if(i != from) buf.append(',');
			buf.append( SQLTokenizer.getKeyWord(i) );
		}
		return buf.toString();
    }
    
    
    public String getNumericFunctions() {
    	return getFunctions(SQLTokenizer.ABS, SQLTokenizer.TRUNCATE);
    }
    
    
    public String getStringFunctions() {
		return getFunctions(SQLTokenizer.ASCII, SQLTokenizer.UCASE);
    }
    
    
    public String getSystemFunctions() {
		return getFunctions(SQLTokenizer.IFNULL, SQLTokenizer.IIF);
    }
    
    
    public String getTimeDateFunctions() {
		return getFunctions(SQLTokenizer.CURDATE, SQLTokenizer.YEAR);
    }
    
    
    public String getSearchStringEscape() {
    	return "\\";
    }
    
    
    public String getExtraNameCharacters() {
    	return "#$ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞßàáâãäåæçèéêëìíîïðñòóôõöøùúûüýþÿ";
    }
	
	
    public boolean supportsAlterTableWithAddColumn() {
        /**@todo: Implement this java.sql.DatabaseMetaData method*/
        throw new java.lang.UnsupportedOperationException("Method supportsAlterTableWithAddColumn() not yet implemented.");
    }
    public boolean supportsAlterTableWithDropColumn() {
        /**@todo: Implement this java.sql.DatabaseMetaData method*/
        throw new java.lang.UnsupportedOperationException("Method supportsAlterTableWithDropColumn() not yet implemented.");
    }
	
	
    public boolean supportsColumnAliasing() {
    	return true;
    }
	
	
    public boolean nullPlusNonNullIsNull() {
    	return true;
    }
	
	
    public boolean supportsConvert() {
    	return true;
    }
	
	
    public boolean supportsConvert(int fromType, int toType) {
    	return true;
    }
	
	
    public boolean supportsTableCorrelationNames() {
    	return true;
    }
	
	
    public boolean supportsDifferentTableCorrelationNames() {
    	return true;
    }
	
	
    public boolean supportsExpressionsInOrderBy() {
    	return true;
    }
	
	
    public boolean supportsOrderByUnrelated() {
    	return true;
    }
	
	
    public boolean supportsGroupBy() {
    	return true;
    }
	
	
    public boolean supportsGroupByUnrelated() {
    	return true;
    }
	
	
    public boolean supportsGroupByBeyondSelect() {
    	return true;
    }
	
	
    public boolean supportsLikeEscapeClause() {
    	return true;
    }
	
	
    public boolean supportsMultipleResultSets() {
    	return true;
    }
	
	
    public boolean supportsMultipleTransactions() {
    	return true;
    }
	
	
    public boolean supportsNonNullableColumns() {
    	return true;
    }
	
	
    public boolean supportsMinimumSQLGrammar() {
    	return true;
    }
	
	
    public boolean supportsCoreSQLGrammar() {
		return true;
    }
	
	
    public boolean supportsExtendedSQLGrammar() {
    	return true;
    }
	
	
    public boolean supportsANSI92EntryLevelSQL() {
    	return true;
    }
	
	
    public boolean supportsANSI92IntermediateSQL() {
    	return true;
    }
	
	
    public boolean supportsANSI92FullSQL() {
    	return true;
    }
	
	
    public boolean supportsIntegrityEnhancementFacility() {
    	return true;
    }
	
	
    public boolean supportsOuterJoins() {
    	return true;
    }
	
	
    public boolean supportsFullOuterJoins() {
    	return true;
    }
	
	
    public boolean supportsLimitedOuterJoins() {
    	return true;
    }
	
	
    public String getSchemaTerm() {
    	return "owner";
    }
	
	
    public String getProcedureTerm() {
    	return "procedure";
    }
	
	
    public String getCatalogTerm() {
    	return "database";
    }
	
	
    public boolean isCatalogAtStart() {
    	return true;
    }
	
	
    public String getCatalogSeparator() {
    	return ".";
    }
	
	
    public boolean supportsSchemasInDataManipulation() {
    	return false;
    }
	
	
    public boolean supportsSchemasInProcedureCalls() {
    	return false;
    }
	
	
    public boolean supportsSchemasInTableDefinitions() {
    	return false;
    }
	
	
    public boolean supportsSchemasInIndexDefinitions() {
    	return false;
    }
	
	
    public boolean supportsSchemasInPrivilegeDefinitions() {
    	return false;
    }
	
	
    public boolean supportsCatalogsInDataManipulation() {
		return true;
    }
	
	
    public boolean supportsCatalogsInProcedureCalls() {
    	return true;
    }
	
	
    public boolean supportsCatalogsInTableDefinitions() {
    	return true;
    }
	
	
    public boolean supportsCatalogsInIndexDefinitions() {
		return true;
    }
	
	
    public boolean supportsCatalogsInPrivilegeDefinitions() {
		return true;
    }
	
	
    public boolean supportsPositionedDelete() {
    	return true;
    }
	
	
    public boolean supportsPositionedUpdate() {
		return true;
    }
	
	
    public boolean supportsSelectForUpdate() {
		return true;
    }
	
	
    public boolean supportsStoredProcedures() {
    	return false;
    }
	
	
    public boolean supportsSubqueriesInComparisons() {
		return true;
    }
	
	
    public boolean supportsSubqueriesInExists() {
		return true;
    }
	
	
    public boolean supportsSubqueriesInIns() {
		return true;
    }
	
	
    public boolean supportsSubqueriesInQuantifieds() {
		return true;
    }
	
	
    public boolean supportsCorrelatedSubqueries() {
		return true;
    }
	
	
    public boolean supportsUnion() {
		return true;
    }
	
	
    public boolean supportsUnionAll() {
		return true;
    }
	
	
    public boolean supportsOpenCursorsAcrossCommit() {
    	return true;
    }
	
	
    public boolean supportsOpenCursorsAcrossRollback() {
    	return true;
    }
	
	
    public boolean supportsOpenStatementsAcrossCommit() {
    	return true;
    }
	
	
    public boolean supportsOpenStatementsAcrossRollback() {
    	return true;
    }
	
	
    public int getMaxBinaryLiteralLength() {
    	return 0;
    }
	
	
    public int getMaxCharLiteralLength() {
    	return 0;
    }
	
	
    public int getMaxColumnNameLength() {
    	return 255;
    }
	
	
    public int getMaxColumnsInGroupBy() {
    	return 0;
    }
	
	
    public int getMaxColumnsInIndex() {
    	return 0;
    }
	
	
    public int getMaxColumnsInOrderBy() {
    	return 0;
    }
	
	
    public int getMaxColumnsInSelect() {
    	return 0;
    }
	
	
    public int getMaxColumnsInTable() {
    	return 0;
    }
	
	
    public int getMaxConnections() {
    	return 0;
    }
	
	
    public int getMaxCursorNameLength() {
    	return 0;
    }
	
	
    public int getMaxIndexLength() {
    	return 0;
    }
	
	
    public int getMaxSchemaNameLength() {
    	return 255;
    }
	
	
    public int getMaxProcedureNameLength() {
    	return 255;
    }
	
	
    public int getMaxCatalogNameLength() {
    	return 255;
    }
	
	
    public int getMaxRowSize() {
    	return 0;
    }
	
	
    public boolean doesMaxRowSizeIncludeBlobs() {
    	return false;
    }
	
	
    public int getMaxStatementLength() {
    	return 0;
    }
	
	
    public int getMaxStatements() {
    	return 0;
    }
	
	
    public int getMaxTableNameLength() {
    	return 255;
    }
	
	
    public int getMaxTablesInSelect() {
    	return 0;
    }
	
	
    public int getMaxUserNameLength() {
    	return 0;
    }
	
	
    public int getDefaultTransactionIsolation() {
    	return Connection.TRANSACTION_READ_COMMITTED;
    }
	
	
    public boolean supportsTransactions() {
    	return true;
    }
	
	
    public boolean supportsTransactionIsolationLevel(int level) {
    	switch(level){
			case Connection.TRANSACTION_NONE:
			case Connection.TRANSACTION_READ_UNCOMMITTED:
    		case Connection.TRANSACTION_READ_COMMITTED:
			case Connection.TRANSACTION_REPEATABLE_READ:
			case Connection.TRANSACTION_SERIALIZABLE:
				return true;
    	}
    	return false;
    }
	
	
    public boolean supportsDataDefinitionAndDataManipulationTransactions() {
    	return true;
    }
	
	
    public boolean supportsDataManipulationTransactionsOnly() {
    	return false;
    }
	
	
    public boolean dataDefinitionCausesTransactionCommit() {
    	return false;
    }
	
	
    public boolean dataDefinitionIgnoredInTransactions() {
    	return false;
    }
	
	
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
		String[] colNames = {"PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "", "", "", "REMARKS", "PROCEDURE_TYPE"};  
		Object[][] data   = new Object[0][];
		return new SSResultSet( st, Utils.createMemoryCommandSelect( con, colNames, data));
    }
	
	
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
		String[] colNames = {"PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "COLUMN_NAME", "COLUMN_TYPE", "DATA_TYPE", "TYPE_NAME", "PRECISION", "LENGTH", "SCALE", "RADIX", "NULLABLE", "REMARKS" };
		Object[][] data   = new Object[0][];
		return new SSResultSet( st, Utils.createMemoryCommandSelect( con, colNames, data));
    }
	
	
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
		String[] colNames = {"TABLE_CAT","TABLE_SCHEM","TABLE_NAME","TABLE_TYPE","REMARKS","TYPE_CAT","TYPE_SCHEM","TYPE_NAME","SELF_REFERENCING_COL_NAME","REF_GENERATION"};
		Database database;
		if(catalog == null){ 
			database = con.getDatabase(true);
			if(database != null)
				catalog = database.getName();
    	}else{
			database = Database.getDatabase(catalog, con, false);
    	}
		ArrayList rows = new ArrayList();
		boolean isTypeTable = types == null;
		boolean isTypeView = types == null;
		for(int i=0; types != null && i<types.length; i++){
			if("TABLE".equalsIgnoreCase(types[i])) isTypeTable = true;
			if("VIEW" .equalsIgnoreCase(types[i])) isTypeView  = true;
		}
		
		if(database != null){
			Strings tables = database.getTables(tableNamePattern);
			for(int i=0; i<tables.size(); i++){
				String table = tables.get(i);
				Object[] row = new Object[10];
				row[0] = catalog;
				row[2] = table;
				try{
					if(database.getTableView( con, table) instanceof View){
						if(isTypeView){
							row[3] = "VIEW";
							rows.add(row);
						}
					}else{
						if(isTypeTable){
							row[3] = "TABLE";					
							rows.add(row);
						}
					}
				}catch(Exception e){
					//TODO invalid VIEWS does not show because it can't load.
				}
			}
		}
		Object[][] data = new Object[rows.size()][];
		rows.toArray(data);
		CommandSelect cmdSelect = Utils.createMemoryCommandSelect( con, colNames, data);
		Expressions order = new Expressions();
		order.add( new ExpressionName("TABLE_TYPE") );
		order.add( new ExpressionName("TABLE_NAME") );
		cmdSelect.setOrder( order );
		return new SSResultSet( st, cmdSelect);
    }
	
	
    public ResultSet getSchemas() throws SQLException {
		String[] colNames = {"TABLE_SCHEM"};
		Object[][] data   = new Object[0][];
		return new SSResultSet( st, Utils.createMemoryCommandSelect( con, colNames, data));
    }
	
    
    public ResultSet getCatalogs() throws SQLException {
    	String[] colNames = {"TABLE_CAT"};
    	Object[][] data   = Database.getCatalogs(con.getDatabase(true));
    	return new SSResultSet( st, Utils.createMemoryCommandSelect( con, colNames, data));
    }
	
    
    public ResultSet getTableTypes() throws SQLException {
		String[] colNames = {"TABLE_TYPE"};
		Object[][] data   = {{"SYSTEM TABLE"}, {"TABLE"}, {"VIEW"}};
		return new SSResultSet( st, Utils.createMemoryCommandSelect( con, colNames, data));
    }
	
    
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
		try {
			String[] colNames = {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE"};
			Object[][] data   = con.getDatabase(false).getColumns(con, tableNamePattern, columnNamePattern);
			return new SSResultSet( st, Utils.createMemoryCommandSelect( con, colNames, data));
		} catch (Exception e) {
			throw SmallSQLException.createFromException(e);
		}
    }
	
	
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        String[] colNames = {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "GRANTOR", "GRANTEE", "PRIVILEGE", "IS_GRANTABLE"};
        /**@todo: Implement this java.sql.DatabaseMetaData method*/
        throw new java.lang.UnsupportedOperationException("Method getColumnPrivileges() not yet implemented.");
    }
    
    
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        String[] colNames = {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "GRANTOR", "GRANTEE", "PRIVILEGE", "IS_GRANTABLE"};
        /**@todo: Implement this java.sql.DatabaseMetaData method*/
        throw new java.lang.UnsupportedOperationException("Method getTablePrivileges() not yet implemented.");
    }
	
	
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
		try {
			String[] colNames = {"SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "PSEUDO_COLUMN"};
			Object[][] data   = con.getDatabase(false).getBestRowIdentifier(con, table);
			return new SSResultSet( st, Utils.createMemoryCommandSelect( con, colNames, data));
		} catch (Exception e) {
			throw SmallSQLException.createFromException(e);
		}
    }
	
	
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
		try {
			String[] colNames = {"SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "PSEUDO_COLUMN"};
			Object[][] data   = new Object[0][0];
			return new SSResultSet( st, Utils.createMemoryCommandSelect( con, colNames, data));
		} catch (Exception e) {
			throw SmallSQLException.createFromException(e);
		}
    }
	
	
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
		try {
			String[] colNames = {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME"};
			Object[][] data   = con.getDatabase(false).getPrimaryKeys(con, table);
			return new SSResultSet( st, Utils.createMemoryCommandSelect( con, colNames, data));
		} catch (Exception e) {
			throw SmallSQLException.createFromException(e);
		}
    }
	
	
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
		return getCrossReference( null, null, null, null, null, table );
    }
	
	
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
		return getCrossReference( null, null, table, null, null, null );
    }
	
	
    public ResultSet getCrossReference(String primaryCatalog, String primarySchema, String primaryTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
		try {
			String[] colNames = {"PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME", "DEFERRABILITY"};
			Object[][] data   = con.getDatabase(false).getReferenceKeys(con, primaryTable, foreignTable);
			return new SSResultSet( st, Utils.createMemoryCommandSelect( con, colNames, data));
		} catch (Exception e) {
			throw SmallSQLException.createFromException(e);
		}
    }
	
	
    public ResultSet getTypeInfo() throws SQLException {
		String[] colNames = {		"TYPE_NAME", 				"DATA_TYPE", 																	"PRECISION", 	"LITERAL_PREFIX", "LITERAL_SUFFIX", 		"CREATE_PARAMS", "NULLABLE", 	 "CASE_SENSITIVE", "SEARCHABLE", "UNSIGNED_ATTRIBUTE", "FIXED_PREC_SCALE", "AUTO_INCREMENT", "LOCAL_TYPE_NAME", "MINIMUM_SCALE", "MAXIMUM_SCALE", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "NUM_PREC_RADIX"};
		Object[][] data   = {
		 {SQLTokenizer.getKeyWord(SQLTokenizer.UNIQUEIDENTIFIER),Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.UNIQUEIDENTIFIER)), Utils.getInteger(36),      	"'",  "'",  null, 				Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), null,          Boolean.FALSE, Boolean.FALSE, null, null,                null,                null, null, null},
		 {SQLTokenizer.getKeyWord(SQLTokenizer.BIT),             Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.BIT) ),             Utils.getInteger(1),      	null, null, null, 				Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), null,          Boolean.FALSE, Boolean.FALSE, null, Utils.getInteger(0), Utils.getInteger(0), null, null, null},
		 {SQLTokenizer.getKeyWord(SQLTokenizer.TINYINT),         Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.TINYINT) ),         Utils.getInteger(3),      	null, null, null, 				Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), Boolean.TRUE,  Boolean.FALSE, Boolean.FALSE, null, Utils.getInteger(0), Utils.getInteger(0), null, null, null},
		 {SQLTokenizer.getKeyWord(SQLTokenizer.BIGINT),          Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.BIGINT) ),          Utils.getInteger(19),     	null, null, null, 				Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, null, Utils.getInteger(0), Utils.getInteger(0), null, null, null},
		 {SQLTokenizer.getKeyWord(SQLTokenizer.LONGVARBINARY),   Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.LONGVARBINARY) ),   Utils.getInteger(2147483647),	"0x", null, null, 		 		Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), null, 			Boolean.FALSE, Boolean.FALSE, null, null, 				 null, 				  null, null, null},
		 {SQLTokenizer.getKeyWord(SQLTokenizer.VARBINARY),   	 Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.VARBINARY) ),   	  Utils.getInteger(65535),	    "0x", null, "max length", 		Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), null, 			Boolean.FALSE, Boolean.FALSE, null, null, 				 null, 				  null, null, null},
		 {SQLTokenizer.getKeyWord(SQLTokenizer.BINARY),   	 	 Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.BINARY) ),   	  	  Utils.getInteger(65535),	    "0x", null, "length", 			Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), null, 			Boolean.FALSE, Boolean.FALSE, null, null, 				 null, 				  null, null, null},
		 {SQLTokenizer.getKeyWord(SQLTokenizer.LONGVARCHAR),     Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.LONGVARCHAR) ),     Utils.getInteger(2147483647),	"'",  "'",  null, 		 		Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), null, 			Boolean.FALSE, Boolean.FALSE, null, null, 				 null, 				  null, null, null},
		 {SQLTokenizer.getKeyWord(SQLTokenizer.LONGNVARCHAR),    Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.LONGNVARCHAR) ),    Utils.getInteger(2147483647),	"'",  "'",  null, 		 		Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), null, 			Boolean.FALSE, Boolean.FALSE, null, null, 				 null, 				  null, null, null},
		 {SQLTokenizer.getKeyWord(SQLTokenizer.CHAR),         	 Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.CHAR) ),         	  Utils.getInteger(65535),   	"'",  "'",  "length", 			Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), null, 			Boolean.FALSE, Boolean.FALSE, null, null, 				 null, 				  null, null, null},
		 {SQLTokenizer.getKeyWord(SQLTokenizer.NCHAR),         	 Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.NCHAR) ),           Utils.getInteger(65535),   	"'",  "'",  "length", 			Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), null, 			Boolean.FALSE, Boolean.FALSE, null, null, 				 null, 				  null, null, null},
		 {SQLTokenizer.getKeyWord(SQLTokenizer.NUMERIC),         Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.NUMERIC) ),         Utils.getInteger(38),     	null, null, "precision,scale", 	Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, null, Utils.getInteger(0), Utils.getInteger(38),null, null, null},
		 {SQLTokenizer.getKeyWord(SQLTokenizer.DECIMAL),         Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.DECIMAL) ),         Utils.getInteger(38),     	null, null, "precision,scale", 	Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, null, Utils.getInteger(0), Utils.getInteger(38),null, null, null},
		 {SQLTokenizer.getKeyWord(SQLTokenizer.MONEY),           Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.MONEY) ),           Utils.getInteger(19),     	null, null, null, 				Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, null, Utils.getInteger(4), Utils.getInteger(4), null, null, null},
		 {SQLTokenizer.getKeyWord(SQLTokenizer.SMALLMONEY),      Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.SMALLMONEY) ),      Utils.getInteger(10),     	null, null, null, 				Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, null, Utils.getInteger(4), Utils.getInteger(4), null, null, null},
		 {SQLTokenizer.getKeyWord(SQLTokenizer.INT),             Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.INT) ),             Utils.getInteger(10),     	null, null, null, 				Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, null, Utils.getInteger(0), Utils.getInteger(0), null, null, null},
		 {SQLTokenizer.getKeyWord(SQLTokenizer.SMALLINT),        Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.SMALLINT) ),        Utils.getInteger(5),      	null, null, null, 				Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, null, Utils.getInteger(0), Utils.getInteger(0), null, null, null},
		 {SQLTokenizer.getKeyWord(SQLTokenizer.FLOAT),        	 Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.FLOAT) ),           Utils.getInteger(15),      	null, null, null, 				Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, null, Utils.getInteger(0), Utils.getInteger(0), null, null, null},
		 {SQLTokenizer.getKeyWord(SQLTokenizer.REAL),        	 Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.REAL) ),        	  Utils.getInteger(7),      	null, null, null, 				Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, null, Utils.getInteger(0), Utils.getInteger(0), null, null, null},
		 {SQLTokenizer.getKeyWord(SQLTokenizer.DOUBLE),          Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.DOUBLE) ),          Utils.getInteger(15),      	null, null, null, 				Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, null, Utils.getInteger(0), Utils.getInteger(0), null, null, null},
		 {SQLTokenizer.getKeyWord(SQLTokenizer.VARCHAR),         Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.VARCHAR) ),         Utils.getInteger(65535),   	"'",  "'",  "max length", 		Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), null, 			Boolean.FALSE, Boolean.FALSE, null, null, 				 null, 				  null, null, null},
		 {SQLTokenizer.getKeyWord(SQLTokenizer.NVARCHAR),        Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.NVARCHAR) ),        Utils.getInteger(65535),   	"'",  "'",  "max length", 		Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), null, 			Boolean.FALSE, Boolean.FALSE, null, null, 				 null, 				  null, null, null},
		 {SQLTokenizer.getKeyWord(SQLTokenizer.BOOLEAN),         Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.BOOLEAN) ),         Utils.getInteger(1),      	null, null, null, 				Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), null,          Boolean.FALSE, Boolean.FALSE, null, Utils.getInteger(0), Utils.getInteger(0), null, null, null},
		 {SQLTokenizer.getKeyWord(SQLTokenizer.DATE),   	 	 Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.DATE) ), 	  		  Utils.getInteger(10),	    	"'",  "'",  null, 				Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), null, 			Boolean.FALSE, Boolean.FALSE, null, null, 				 null, 				  null, null, null},
		 {SQLTokenizer.getKeyWord(SQLTokenizer.TIME),   	 	 Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.TIME) ), 	  		  Utils.getInteger(8),	    	"'",  "'",  null, 				Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), null, 			Boolean.FALSE, Boolean.FALSE, null, null, 				 null, 				  null, null, null},
		 {SQLTokenizer.getKeyWord(SQLTokenizer.TIMESTAMP),   	 Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.TIMESTAMP) ), 	  Utils.getInteger(23),	    	"'",  "'",  null, 				Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), null, 			Boolean.FALSE, Boolean.FALSE, null, Utils.getInteger(3), Utils.getInteger(3), null, null, null},
		 {SQLTokenizer.getKeyWord(SQLTokenizer.SMALLDATETIME),   Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.SMALLDATETIME) ),   Utils.getInteger(16),	    	"'",  "'",  null, 				Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), null, 			Boolean.FALSE, Boolean.FALSE, null, null, 				 null, 				  null, null, null},
		 {SQLTokenizer.getKeyWord(SQLTokenizer.JAVA_OBJECT),   	 Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.JAVA_OBJECT) ),     Utils.getInteger(65535),	    null, null, null, 				Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), null, 			Boolean.FALSE, Boolean.FALSE, null, null, 				 null, 				  null, null, null},
		 {SQLTokenizer.getKeyWord(SQLTokenizer.BLOB),   		 Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.BLOB) ),   		  Utils.getInteger(2147483647),	"0x", null, null, 		 		Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), null, 			Boolean.FALSE, Boolean.FALSE, null, null, 				 null, 				  null, null, null},
		 {SQLTokenizer.getKeyWord(SQLTokenizer.CLOB),     		 Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.CLOB) ),     		  Utils.getInteger(2147483647),	"'",  "'",  null, 		 		Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), null, 			Boolean.FALSE, Boolean.FALSE, null, null, 				 null, 				  null, null, null},
		 {SQLTokenizer.getKeyWord(SQLTokenizer.NCLOB),     		 Utils.getShort(SQLTokenizer.getSQLDataType( SQLTokenizer.NCLOB) ),     	  Utils.getInteger(2147483647),	"'",  "'",  null, 		 		Utils.getShort(typeNullable), Boolean.FALSE, Utils.getShort(typeSearchable), null, 			Boolean.FALSE, Boolean.FALSE, null, null, 				 null, 				  null, null, null},
		};
		//TODO add more data types to the list
		return new SSResultSet( st, Utils.createMemoryCommandSelect( con, colNames, data));
    }
	
	
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
		try {
			String[] colNames = {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "NON_UNIQUE", "INDEX_QUALIFIER", "INDEX_NAME", "TYPE", "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC", "CARDINALITY", "PAGES", "FILTER_CONDITION"};
			Object[][] data   = con.getDatabase(false).getIndexInfo(con, table, unique);
			return new SSResultSet( st, Utils.createMemoryCommandSelect( con, colNames, data));
		} catch (Exception e) {
			throw SmallSQLException.createFromException(e);
		}
    }
	
	
    public boolean supportsResultSetType(int type) {
		switch(type){
			case ResultSet.TYPE_FORWARD_ONLY:
			case ResultSet.TYPE_SCROLL_INSENSITIVE:
			case ResultSet.TYPE_SCROLL_SENSITIVE:
				return true;
		}
		return false;
    }
	
	
    public boolean supportsResultSetConcurrency(int type, int concurrency) {
		if(type >= ResultSet.TYPE_FORWARD_ONLY && type <= ResultSet.TYPE_SCROLL_SENSITIVE &&
			concurrency >= ResultSet.CONCUR_READ_ONLY && concurrency <= ResultSet.CONCUR_UPDATABLE)
			return true;
		return false;
    }
	
	
    public boolean ownUpdatesAreVisible(int type) {
		return supportsResultSetType(type);
    }
	
	
    public boolean ownDeletesAreVisible(int type) {
		return supportsResultSetType(type);
    }

	
	public boolean ownInsertsAreVisible(int type) {
		return supportsResultSetType(type);
    }
	
	
    public boolean othersUpdatesAreVisible(int type) {
		return supportsResultSetType(type);
    }
	
	
    public boolean othersDeletesAreVisible(int type) {
		return supportsResultSetType(type);
    }
	
	
    public boolean othersInsertsAreVisible(int type) {
		return supportsResultSetType(type);
    }
	
	
    public boolean updatesAreDetected(int type) {
		return false;
    }
	
	
    public boolean deletesAreDetected(int type) {
		return supportsResultSetType(type);
    }
	
	
    public boolean insertsAreDetected(int type) {
		return supportsResultSetType(type);
    }
	
	
    public boolean supportsBatchUpdates() {
		return true;
    }
	
	
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
		String[] colNames = {"TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "CLASS_NAME", "DATA_TYPE", "REMARKS"};        
		Object[][] data   = new Object[0][];
		return new SSResultSet( st, Utils.createMemoryCommandSelect( con, colNames, data));
    }
	
	
    public Connection getConnection() {
    	return con;
    }
	
	
    public boolean supportsSavepoints() {
    	return false;
    }
	
	
    public boolean supportsNamedParameters() {
		return true;
    }
	
	
    public boolean supportsMultipleOpenResults() {
		return true;
    }
	
	
    public boolean supportsGetGeneratedKeys() {
		return true;
    }
	
	
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        /**@todo: Implement this java.sql.DatabaseMetaData method*/
        throw new java.lang.UnsupportedOperationException("Method getSuperTypes() not yet implemented.");
    }
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        /**@todo: Implement this java.sql.DatabaseMetaData method*/
        throw new java.lang.UnsupportedOperationException("Method getSuperTables() not yet implemented.");
    }
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        /**@todo: Implement this java.sql.DatabaseMetaData method*/
        throw new java.lang.UnsupportedOperationException("Method getAttributes() not yet implemented.");
    }
	
	
    public boolean supportsResultSetHoldability(int holdability) {
		return true;
    }
	
	
    public int getResultSetHoldability() {
		return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }
	
	
    public int getDatabaseMajorVersion() {
    	return getDriverMajorVersion();
    }
	
	
    public int getDatabaseMinorVersion() {
		return getDriverMinorVersion();
    }
	
	
    public int getJDBCMajorVersion() {
    	return 3;
    }
	
	
    public int getJDBCMinorVersion() {
    	return 0;
    }
	
	
    public int getSQLStateType() {
		return sqlStateSQL99;
    }
	
	
    public boolean locatorsUpdateCopy() {
		return false;
    }
	
	
    public boolean supportsStatementPooling() {
		return false;
    }
}