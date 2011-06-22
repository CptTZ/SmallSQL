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
 * Language.java
 * ---------------
 * Author: Volker Berlin
 * Author: Saverio Miroddi 
 */
package smallsql.database.language;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Base localization class.<br>
 * Contains English default messages and ResourceBundle interface
 * implementation.
 */
public class Language {
	//////////////////////////////////////////////////////////////////////
	// NAMED CONSTANTS 
	//////////////////////////////////////////////////////////////////////
	
	/* CUSTOM_MESSAGE doesn't need to be copied to subclasses. Also, it's
	 * not checked by the difference() method. */
	public static final String CUSTOM_MESSAGE			= "SS-0000";
	
	public static final String UNSUPPORTED_OPERATION 	= "SS-0001";
    public static final String CANT_LOCK_FILE           = "SS-0003";

	public static final String DB_EXISTENT 				= "SS-0030";
	public static final String DB_NONEXISTENT 			= "SS-0031";
	public static final String DB_NOT_DIRECTORY 		= "SS-0032";
	public static final String DB_NOTCONNECTED 			= "SS-0033";
	public static final String DB_READONLY              = "SS-0034";

	public static final String CONNECTION_CLOSED 		= "SS-0070";

	public static final String VIEW_INSERT 				= "SS-0100";
	public static final String VIEWDROP_NOT_VIEW 		= "SS-0101";
	public static final String VIEW_CANTDROP 			= "SS-0102";

	public static final String RSET_NOT_PRODUCED 		= "SS-0130";
	public static final String RSET_READONLY 			= "SS-0131";
	public static final String RSET_FWDONLY				= "SS-0132";
	public static final String RSET_CLOSED				= "SS-0133";
	public static final String RSET_NOT_INSERT_ROW		= "SS-0134";
	public static final String RSET_ON_INSERT_ROW		= "SS-0135";
	public static final String ROWSOURCE_READONLY		= "SS-0136";
    
    public static final String STMT_IS_CLOSED           = "SS-0140";

	public static final String SUBQUERY_COL_COUNT		= "SS-0160";
	public static final String JOIN_DELETE				= "SS-0161";
	public static final String JOIN_INSERT				= "SS-0162";
	public static final String DELETE_WO_FROM			= "SS-0163";
	public static final String INSERT_WO_FROM			= "SS-0164";

	public static final String TABLE_CANT_RENAME		= "SS-0190";
	public static final String TABLE_CANT_DROP			= "SS-0191";
	public static final String TABLE_CANT_DROP_LOCKED	= "SS-0192";
	public static final String TABLE_CORRUPT_PAGE		= "SS-0193";
	public static final String TABLE_MODIFIED			= "SS-0194";
	public static final String TABLE_DEADLOCK			= "SS-0195";
	public static final String TABLE_OR_VIEW_MISSING	= "SS-0196";
	public static final String TABLE_FILE_INVALID		= "SS-0197";
	public static final String TABLE_OR_VIEW_FILE_INVALID = "SS-0198";
	public static final String TABLE_EXISTENT			= "SS-0199";

	public static final String FK_NOT_TABLE				= "SS-0220";
	public static final String PK_ONLYONE				= "SS-0221";
	public static final String KEY_DUPLICATE			= "SS-0222";

	public static final String MONTH_TOOLARGE 			= "SS-0251";
	public static final String DAYS_TOOLARGE 			= "SS-0252";
	public static final String HOURS_TOOLARGE 			= "SS-0253";
	public static final String MINUTES_TOOLARGE 		= "SS-0254";
	public static final String SECS_TOOLARGE 			= "SS-0255";
	public static final String MILLIS_TOOLARGE 			= "SS-0256";
	public static final String DATETIME_INVALID 		= "SS-0257";

	public static final String UNSUPPORTED_CONVERSION_OPER = "SS-0280";
	public static final String UNSUPPORTED_DATATYPE_OPER = "SS-0281";
	public static final String UNSUPPORTED_DATATYPE_FUNC = "SS-0282";
	public static final String UNSUPPORTED_CONVERSION_FUNC = "SS-0283";
	public static final String UNSUPPORTED_TYPE_CONV 	= "SS-0284";
	public static final String UNSUPPORTED_TYPE_SUM 	= "SS-0285";
	public static final String UNSUPPORTED_TYPE_MAX 	= "SS-0286";
	public static final String UNSUPPORTED_CONVERSION 	= "SS-0287";
	public static final String INSERT_INVALID_LEN 		= "SS-0288";
	public static final String SUBSTR_INVALID_LEN 		= "SS-0289";

	public static final String VALUE_STR_TOOLARGE 		= "SS-0310";
	public static final String VALUE_BIN_TOOLARGE 		= "SS-0311";
	public static final String VALUE_NULL_INVALID 		= "SS-0312";
	public static final String VALUE_CANT_CONVERT 		= "SS-0313";

	public static final String BYTEARR_INVALID_SIZE 	= "SS-0340";
	public static final String LOB_DELETED 				= "SS-0341";

	public static final String PARAM_CLASS_UNKNOWN 		= "SS-0370";
	public static final String PARAM_EMPTY 				= "SS-0371";
	public static final String PARAM_IDX_OUT_RANGE 		= "SS-0372";

	public static final String COL_DUPLICATE 			= "SS-0400";
	public static final String COL_MISSING 				= "SS-0401";
	public static final String COL_VAL_UNMATCH 			= "SS-0402";
	public static final String COL_INVALID_SIZE 		= "SS-0403";
	public static final String COL_WRONG_PREFIX 		= "SS-0404";
	public static final String COL_READONLY 			= "SS-0405";
	public static final String COL_INVALID_NAME 		= "SS-0406";
	public static final String COL_IDX_OUT_RANGE 		= "SS-0407";
	public static final String COL_AMBIGUOUS 			= "SS-0408";
	
	public static final String GROUP_AGGR_INVALID 		= "SS-0430";
	public static final String GROUP_AGGR_NOTPART 		= "SS-0431";
	public static final String ORDERBY_INTERNAL 		= "SS-0432";
	public static final String UNION_DIFFERENT_COLS 	= "SS-0433";

	public static final String INDEX_EXISTS 			= "SS-0460";
	public static final String INDEX_MISSING 			= "SS-0461";
	public static final String INDEX_FILE_INVALID 		= "SS-0462";
	public static final String INDEX_CORRUPT 			= "SS-0463";
	public static final String INDEX_TOOMANY_EQUALS 	= "SS-0464";

	public static final String FILE_TOONEW 				= "SS-0490";
	public static final String FILE_TOOOLD 				= "SS-0491";
    public static final String FILE_CANT_DELETE         = "SS-0492";

	public static final String ROW_0_ABSOLUTE 			= "SS-0520";
	public static final String ROW_NOCURRENT 			= "SS-0521";
	public static final String ROWS_WRONG_MAX 			= "SS-0522";
	public static final String ROW_LOCKED 				= "SS-0523";
	public static final String ROW_DELETED 				= "SS-0524";

	public static final String SAVEPT_INVALID_TRANS 	= "SS-0550";
	public static final String SAVEPT_INVALID_DRIVER 	= "SS-0551";

	public static final String ALIAS_UNSUPPORTED 		= "SS-0580";
	public static final String ISOLATION_UNKNOWN 		= "SS-0581";
	public static final String FLAGVALUE_INVALID 		= "SS-0582";
	public static final String ARGUMENT_INVALID 		= "SS-0583";
	public static final String GENER_KEYS_UNREQUIRED 	= "SS-0584";
	public static final String SEQUENCE_HEX_INVALID 	= "SS-0585";
	public static final String SEQUENCE_HEX_INVALID_STR = "SS-0586";
	
	public static final String SYNTAX_BASE_OFS			= "SS-0610";
	public static final String SYNTAX_BASE_END			= "SS-0611";
	public static final String STXADD_ADDITIONAL_TOK	= "SS-0612";
	public static final String STXADD_IDENT_EXPECT		= "SS-0613";
	public static final String STXADD_IDENT_EMPTY		= "SS-0614";
	public static final String STXADD_IDENT_WRONG		= "SS-0615";
	public static final String STXADD_OPER_MINUS		= "SS-0616";	
	public static final String STXADD_FUNC_UNKNOWN		= "SS-0617";	
	public static final String STXADD_PARAM_INVALID_COUNT	= "SS-0618";
	public static final String STXADD_JOIN_INVALID		= "SS-0619";
	public static final String STXADD_FROM_PAR_CLOSE	= "SS-0620";
	public static final String STXADD_KEYS_REQUIRED		= "SS-0621";
	public static final String STXADD_NOT_NUMBER		= "SS-0622";
	public static final String STXADD_COMMENT_OPEN		= "SS-0623";

	//////////////////////////////////////////////////////////////////////
	// VARIABLE, CONSTRUCTOR AND NON-RESOURCEBUNDLE METHODS. 
	//////////////////////////////////////////////////////////////////////
	
	private Map messages;
	private Map sqlStates;
	
	/**
	 * Return a Language instance.<br>
	 * Defaults to ENGLISH language.<br>
	 * Warning: there is difference between locale strings like 'en_EN' and
	 * 'en_UK': it's advised to pass only the first two characters.
	 * 
	 * @param localeStr
	 *            Locale.toString() value for Language. Nullable, for ENGLISH.
	 * @return Language instance.
	 * @throws InstantiationException
	 *             Error during instantiation, i.e. duplicate entry found.
	 */
	public static Language getLanguage(String localeStr) {
		try {
			return getFromLocaleTree(localeStr);
		}
		catch (IllegalArgumentException e) {
			return getDefaultLanguage();
		}
	}
	
	/**
	 * Gets the language for the default locale; if not found, returns the
	 * ENGLISH language.
	 */
	public static Language getDefaultLanguage() {		
		String dfltLocaleStr = Locale.getDefault().toString();

		try {
			return getFromLocaleTree(dfltLocaleStr);
		}
		catch (IllegalArgumentException e) {
			return new Language(); // default to English
		}
	}
	
	/**
	 * Searches a language in the Locale tree, for example:<br>
	 * first 'en_UK', then 'en'
	 * 
	 * @param localeStr
	 *            locale string.
	 * @return Language instance, if found.
	 * @throws IllegalArgumentException
	 *             Language not found in the tree.
	 */
	private static Language getFromLocaleTree(String localeStr) 
	throws IllegalArgumentException {
		String part = localeStr;
		while (true) {
			String langClassName = Language.class.getName() + '_' + part;
			
			try {
				return (Language) Class.forName(langClassName).newInstance();
			}
			catch (IllegalAccessException e) {
				 assert(false): "Internal error: must never happen.";
			}
			catch (ClassNotFoundException e) { 
				// do nothing
			}
			catch (InstantiationException e) { 
				assert(false): "Error during Language instantiation: " + e.getMessage();
			}
			
			int lastUndsc = part.lastIndexOf("_");
			
			if (lastUndsc > -1) part = part.substring(0, lastUndsc);
			else break;			
		}
		
		throw new IllegalArgumentException("Locale not found in the tree: " + localeStr);
	}

	/**
	 * Base language constructor; fills the internal map with the English
	 * messages
	 */
	protected Language() {
		messages = new HashMap((int)(MESSAGES.length / 0.7)); //avoid rehashing ;-)
		sqlStates = new HashMap((int)(MESSAGES.length / 0.7)); //avoid rehashing ;-)
		addMessages(MESSAGES);
		setSqlStates();
	}
	
	/**
	 * Add entries to message map.<br>
	 * If duplicates entries are found the adding entries, an exception is
	 * thrown.
	 * 
	 * @param entries
	 *            adding language entries.
	 * @throws IllegalArgumentException
	 *             if duplicate entry is found.
	 */
	protected final void addMessages(String[][] entries) 
	throws IllegalArgumentException {
		Set inserted = new HashSet(); // for duplicates checking
		
		for (int i = 0; i < entries.length; i++) {
			String key = entries[i][0];
			
			if (! inserted.add(key)) {
				throw new IllegalArgumentException("Duplicate key: " + key);
			}
			else {
				String value = entries[i][1];
				messages.put(key, value);
			}
		}
	}
	
	/**
	 * Add entries to sql states map.<br>
	 * If duplicates entries are found the adding entries, an exception is
	 * thrown.
	 * 
	 * @param entries
	 *            adding language entries.
	 * @throws IllegalArgumentException
	 *             if duplicate entry is found.
	 */
	private final void setSqlStates() {
		Set inserted = new HashSet(); // for duplicates checking
		
		for (int i = 0; i < SQL_STATES.length; i++) {
			String key = SQL_STATES[i][0];
			
			if (! inserted.add(key)) {
				throw new IllegalArgumentException("Duplicate key: " + key);
			}
			else {
				String value = SQL_STATES[i][1];
				sqlStates.put(key, value);
			}
		}
	}

	public String getMessage(String key) {
		String message = (String) messages.get(key);
		assert(message != null): "Message code not found: " + key;
		return message;
	}

	public String getSqlState(String key) {
		String sqlState = (String) sqlStates.get(key);
		assert(sqlState != null): "SQL State code not found: " + key;
		return sqlState;
	}

	public String[][] getEntries() {
		return MESSAGES;
	}
	
	//////////////////////////////////////////////////////////////////////
	// MESSAGES
	//////////////////////////////////////////////////////////////////////
	
	private final String[][] MESSAGES = {
{ CUSTOM_MESSAGE           		  , "{0}" },

{ UNSUPPORTED_OPERATION           , "Unsupported Operation {0}." },
{ CANT_LOCK_FILE                  , "Can''t lock file ''{0}''. A single SmallSQL Database can only be opened from a single process." },

{ DB_EXISTENT                     , "Database ''{0}'' already exists." },
{ DB_NONEXISTENT                  , "Database ''{0}'' does not exist." },
{ DB_NOT_DIRECTORY                , "Directory ''{0}'' is not a SmallSQL database." },
{ DB_NOTCONNECTED                 , "You are not connected with a Database." },

{ CONNECTION_CLOSED               , "Connection is already closed." },

{ VIEW_INSERT                     , "INSERT is not supported for a view." },
{ VIEWDROP_NOT_VIEW               , "Cannot use DROP VIEW with ''{0}'' because it does not is a view." },
{ VIEW_CANTDROP                   , "View ''{0}'' can''t be dropped." },

{ RSET_NOT_PRODUCED               , "No ResultSet was produced." },
{ RSET_READONLY                   , "ResultSet is read only." },
{ RSET_FWDONLY                    , "ResultSet is forward only." },
{ RSET_CLOSED                     , "ResultSet is closed." },
{ RSET_NOT_INSERT_ROW             , "Cursor is currently not on the insert row." },
{ RSET_ON_INSERT_ROW              , "Cursor is currently on the insert row." },
{ ROWSOURCE_READONLY              , "Rowsource is read only." },
{ STMT_IS_CLOSED                  , "Statement is already closed." },

{ SUBQUERY_COL_COUNT              , "Count of columns in subquery must be 1 and not {0}." },
{ JOIN_DELETE                     , "The method deleteRow not supported on joins." },
{ JOIN_INSERT                     , "The method insertRow not supported on joins." },
{ DELETE_WO_FROM                  , "The method deleteRow need a FROM expression." },
{ INSERT_WO_FROM                  , "The method insertRow need a FROM expression." },

{ TABLE_CANT_RENAME               , "Table ''{0}'' can''t be renamed." },
{ TABLE_CANT_DROP                 , "Table ''{0}'' can''t be dropped." },
{ TABLE_CANT_DROP_LOCKED          , "Table ''{0}'' can''t drop because is locked." },
{ TABLE_CORRUPT_PAGE              , "Corrupt table page at position: {0}." },
{ TABLE_MODIFIED                  , "Table ''{0}'' was modified." },
{ TABLE_DEADLOCK                  , "Deadlock, can not create a lock on table ''{0}''." },
{ TABLE_OR_VIEW_MISSING           , "Table or View ''{0}'' does not exist." },
{ TABLE_FILE_INVALID              , "File ''{0}'' does not include a valid SmallSQL Table." },
{ TABLE_OR_VIEW_FILE_INVALID      , "File ''{0}'' is not a valid Table or View store." },
{ TABLE_EXISTENT                  , "Table or View ''{0}'' already exists." },

{ FK_NOT_TABLE                    , "''{0}'' is not a table." },
{ PK_ONLYONE                      , "A table can have only one primary key." },
{ KEY_DUPLICATE                   , "Duplicate Key." },

{ MONTH_TOOLARGE                  , "Months are too large in DATE or TIMESTAMP value ''{0}''." },
{ DAYS_TOOLARGE                   , "Days are too large in DATE or TIMESTAMP value ''{0}''." },
{ HOURS_TOOLARGE                  , "Hours are too large in TIME or TIMESTAMP value ''{0}''." },
{ MINUTES_TOOLARGE                , "Minutes are too large in TIME or TIMESTAMP value ''{0}''." },
{ SECS_TOOLARGE                   , "Seconds are too large in TIME or TIMESTAMP value ''{0}''." },
{ MILLIS_TOOLARGE                 , "Milliseconds are too large in TIMESTAMP value ''{0}''." },
{ DATETIME_INVALID                , "''{0}'' is an invalid DATE, TIME or TIMESTAMP." },

{ UNSUPPORTED_CONVERSION_OPER     , "Unsupported conversion to data type ''{0}'' from data type ''{1}'' for operation ''{2}''." },
{ UNSUPPORTED_DATATYPE_OPER       , "Unsupported data type ''{0}'' for operation ''{1}''." },
{ UNSUPPORTED_DATATYPE_FUNC       , "Unsupported data type ''{0}'' for function ''{1}''." },
{ UNSUPPORTED_CONVERSION_FUNC     , "Unsupported conversion to data type ''{0}'' for function ''{1}''." },
{ UNSUPPORTED_TYPE_CONV           , "Unsupported type for CONVERT function: {0}." },
{ UNSUPPORTED_TYPE_SUM            , "Unsupported data type ''{0}'' for SUM function." },
{ UNSUPPORTED_TYPE_MAX            , "Unsupported data type ''{0}'' for MAX function." },
{ UNSUPPORTED_CONVERSION          , "Can''t convert ''{0}'' [{1}] to ''{2}''." },
{ INSERT_INVALID_LEN              , "Invalid length ''{0}'' in function INSERT." },
{ SUBSTR_INVALID_LEN              , "Invalid length ''{0}'' in function SUBSTRING." },

{ VALUE_STR_TOOLARGE              , "String value too large for column." },
{ VALUE_BIN_TOOLARGE              , "Binary value with length {0} to large for column with size {1}." },
{ VALUE_NULL_INVALID              , "Null values are not valid for column ''{0}''." },
{ VALUE_CANT_CONVERT              , "Cannot convert a {0} value to a {1} value." },

{ BYTEARR_INVALID_SIZE            , "Invalid byte array size {0} for UNIQUEIDENFIER." },
{ LOB_DELETED                     , "Lob Object was deleted." },

{ PARAM_CLASS_UNKNOWN             , "Unknown parameter class: ''{0}''." },
{ PARAM_EMPTY                     , "Parameter {0} is empty." },
{ PARAM_IDX_OUT_RANGE             , "Parameter index {0} out of range. The value must be between 1 and {1}." },

{ COL_DUPLICATE                	  , "There is a duplicated column name: ''{0}''." },
{ COL_MISSING                     , "Column ''{0}'' not found." },
{ COL_VAL_UNMATCH                 , "Columns and Values count is not identical." },
{ COL_INVALID_SIZE                , "Invalid column size {0} for column ''{1}''." },
{ COL_WRONG_PREFIX                , "The column prefix ''{0}'' does not match with a table name or alias name used in this query." },
{ COL_READONLY                    , "Column {0} is read only." },
{ COL_INVALID_NAME                , "Invalid column name ''{0}''." },
{ COL_IDX_OUT_RANGE               , "Column index out of range: {0}." },
{ COL_AMBIGUOUS                   , "Column ''{0}'' is ambiguous." },

{ GROUP_AGGR_INVALID              , "Aggregate function are not valid in the GROUP BY clause ({0})." },
{ GROUP_AGGR_NOTPART              , "Expression ''{0}'' is not part of a aggregate function or GROUP BY clause." },
{ ORDERBY_INTERNAL                , "Internal Error with ORDER BY." },
{ UNION_DIFFERENT_COLS            , "Different SELECT of the UNION have different column count: {0} and {1}." },

{ INDEX_EXISTS                    , "Index ''{0}'' already exists." },
{ INDEX_MISSING                   , "Index ''{0}'' does not exist." },
{ INDEX_FILE_INVALID              , "File ''{0}'' is not a valid Index store." },
{ INDEX_CORRUPT                   , "Error in loading Index. Index file is corrupt. ({0})." },
{ INDEX_TOOMANY_EQUALS            , "Too many equals entry in Index." },

{ FILE_TOONEW                     , "File version ({0}) of file ''{1}'' is too new for this runtime." },
{ FILE_TOOOLD                     , "File version ({0}) of file ''{1}'' is too old for this runtime." },
{ FILE_CANT_DELETE                , "File ''{0}'' can't be deleted." },

{ ROW_0_ABSOLUTE                  , "Row 0 is invalid for method absolute()." },
{ ROW_NOCURRENT                   , "No current row." },
{ ROWS_WRONG_MAX                  , "Wrong max rows value: {0}." },
{ ROW_LOCKED                      , "Row is locked from another Connection." },
{ ROW_DELETED                     , "Row already deleted." },

{ SAVEPT_INVALID_TRANS            , "Savepoint is not valid for this transaction." },
{ SAVEPT_INVALID_DRIVER           , "Savepoint is not valid for this driver {0}." },

{ ALIAS_UNSUPPORTED               , "Alias not supported for this type of row source." },
{ ISOLATION_UNKNOWN               , "Unknown Transaction Isolation Level: {0}." },
{ FLAGVALUE_INVALID               , "Invalid flag value in method getMoreResults: {0}." },
{ ARGUMENT_INVALID                , "Invalid argument in method setNeedGenratedKeys: {0}." },
{ GENER_KEYS_UNREQUIRED           , "GeneratedKeys not requested." },
{ SEQUENCE_HEX_INVALID            , "Invalid hex sequence at {0}." },
{ SEQUENCE_HEX_INVALID_STR        , "Invalid hex sequence at position {0} in ''{1}''." },

{ SYNTAX_BASE_OFS            	  , "Syntax error at offset {0} on ''{1}''. " },
{ SYNTAX_BASE_END        		  , "Syntax error, unexpected end of SQL string. " },
{ STXADD_ADDITIONAL_TOK			  , "Additional token after end of SQL statement." },
{ STXADD_IDENT_EXPECT			  , "Identifier expected." },
{ STXADD_IDENT_EMPTY 			  , "Empty Identifier." },
{ STXADD_IDENT_WRONG 			  , "Wrong Identifier ''{0}''." },
{ STXADD_OPER_MINUS 			  , "Invalid operator minus for data type VARBINARY." },
{ STXADD_FUNC_UNKNOWN 			  , "Unknown function." },
{ STXADD_PARAM_INVALID_COUNT	  , "Invalid parameter count." },
{ STXADD_JOIN_INVALID	  		  , "Invalid Join Syntax." },
{ STXADD_FROM_PAR_CLOSE	  		  , "Unexpected closing parenthesis in FROM clause." },
{ STXADD_KEYS_REQUIRED	  		  , "Required keywords are: " },
{ STXADD_NOT_NUMBER	  		      , "Number value required (passed = ''{0}'')." },
{ STXADD_COMMENT_OPEN			  , "Missing end comment mark (''*/'')." },
	};

	//////////////////////////////////////////////////////////////////////
	// SQL_STATES
	//////////////////////////////////////////////////////////////////////
	
	private final String[][] SQL_STATES = {
{ CUSTOM_MESSAGE           		  , "01000" },

{ UNSUPPORTED_OPERATION           , "01000" },
{ CANT_LOCK_FILE                  , "01000" },

{ DB_EXISTENT                     , "01000" },
{ DB_NONEXISTENT                  , "01000" },
{ DB_NOT_DIRECTORY                , "01000" },
{ DB_NOTCONNECTED                 , "01000" },

{ CONNECTION_CLOSED               , "01000" },

{ VIEW_INSERT                     , "01000" },
{ VIEWDROP_NOT_VIEW               , "01000" },
{ VIEW_CANTDROP                   , "01000" },

{ RSET_NOT_PRODUCED               , "01000" },
{ RSET_READONLY                   , "01000" },
{ RSET_FWDONLY                    , "01000" },
{ RSET_CLOSED                     , "01000" },
{ RSET_NOT_INSERT_ROW             , "01000" },
{ RSET_ON_INSERT_ROW              , "01000" },
{ ROWSOURCE_READONLY              , "01000" },
{ STMT_IS_CLOSED                  , "HY010" },

{ SUBQUERY_COL_COUNT              , "01000" },
{ JOIN_DELETE                     , "01000" },
{ JOIN_INSERT                     , "01000" },
{ DELETE_WO_FROM                  , "01000" },
{ INSERT_WO_FROM                  , "01000" },

{ TABLE_CANT_RENAME               , "01000" },
{ TABLE_CANT_DROP                 , "01000" },
{ TABLE_CANT_DROP_LOCKED          , "01000" },
{ TABLE_CORRUPT_PAGE              , "01000" },
{ TABLE_MODIFIED                  , "01000" },
{ TABLE_DEADLOCK                  , "01000" },
{ TABLE_OR_VIEW_MISSING           , "01000" },
{ TABLE_FILE_INVALID              , "01000" },
{ TABLE_OR_VIEW_FILE_INVALID      , "01000" },
{ TABLE_EXISTENT                  , "01000" },

{ FK_NOT_TABLE                    , "01000" },
{ PK_ONLYONE                      , "01000" },
{ KEY_DUPLICATE                   , "01000" },

{ MONTH_TOOLARGE                  , "01000" },
{ DAYS_TOOLARGE                   , "01000" },
{ HOURS_TOOLARGE                  , "01000" },
{ MINUTES_TOOLARGE                , "01000" },
{ SECS_TOOLARGE                   , "01000" },
{ MILLIS_TOOLARGE                 , "01000" },
{ DATETIME_INVALID                , "01000" },

{ UNSUPPORTED_CONVERSION_OPER     , "01000" },
{ UNSUPPORTED_DATATYPE_OPER       , "01000" },
{ UNSUPPORTED_DATATYPE_FUNC       , "01000" },
{ UNSUPPORTED_CONVERSION_FUNC     , "01000" },
{ UNSUPPORTED_TYPE_CONV           , "01000" },
{ UNSUPPORTED_TYPE_SUM            , "01000" },
{ UNSUPPORTED_TYPE_MAX            , "01000" },
{ UNSUPPORTED_CONVERSION          , "01000" },
{ INSERT_INVALID_LEN              , "01000" },
{ SUBSTR_INVALID_LEN              , "01000" },

{ VALUE_STR_TOOLARGE              , "01000" },
{ VALUE_BIN_TOOLARGE              , "01000" },
{ VALUE_NULL_INVALID              , "01000" },
{ VALUE_CANT_CONVERT              , "01000" },

{ BYTEARR_INVALID_SIZE            , "01000" },
{ LOB_DELETED                     , "01000" },

{ PARAM_CLASS_UNKNOWN             , "01000" },
{ PARAM_EMPTY                     , "01000" },
{ PARAM_IDX_OUT_RANGE             , "01000" },

{ COL_DUPLICATE                	  , "01000" },
{ COL_MISSING                     , "01000" },
{ COL_VAL_UNMATCH                 , "01000" },
{ COL_INVALID_SIZE                , "01000" },
{ COL_WRONG_PREFIX                , "01000" },
{ COL_READONLY                    , "01000" },
{ COL_INVALID_NAME                , "01000" },
{ COL_IDX_OUT_RANGE               , "01000" },
{ COL_AMBIGUOUS                   , "01000" },

{ GROUP_AGGR_INVALID              , "01000" },
{ GROUP_AGGR_NOTPART              , "01000" },
{ ORDERBY_INTERNAL                , "01000" },
{ UNION_DIFFERENT_COLS            , "01000" },

{ INDEX_EXISTS                    , "01000" },
{ INDEX_MISSING                   , "01000" },
{ INDEX_FILE_INVALID              , "01000" },
{ INDEX_CORRUPT                   , "01000" },
{ INDEX_TOOMANY_EQUALS            , "01000" },

{ FILE_TOONEW                     , "01000" },
{ FILE_TOOOLD                     , "01000" },
{ FILE_CANT_DELETE                , "01000" },

{ ROW_0_ABSOLUTE                  , "01000" },
{ ROW_NOCURRENT                   , "01000" },
{ ROWS_WRONG_MAX                  , "01000" },
{ ROW_LOCKED                      , "01000" },
{ ROW_DELETED                     , "01000" },

{ SAVEPT_INVALID_TRANS            , "01000" },
{ SAVEPT_INVALID_DRIVER           , "01000" },

{ ALIAS_UNSUPPORTED               , "01000" },
{ ISOLATION_UNKNOWN               , "01000" },
{ FLAGVALUE_INVALID               , "01000" },
{ ARGUMENT_INVALID                , "01000" },
{ GENER_KEYS_UNREQUIRED           , "01000" },
{ SEQUENCE_HEX_INVALID            , "01000" },
{ SEQUENCE_HEX_INVALID_STR        , "01000" },

{ SYNTAX_BASE_OFS            	  , "01000" },
{ SYNTAX_BASE_END        		  , "01000" },
{ STXADD_ADDITIONAL_TOK			  , "01000" },
{ STXADD_IDENT_EXPECT			  , "01000" },
{ STXADD_IDENT_EMPTY 			  , "01000" },
{ STXADD_IDENT_WRONG 			  , "01000" },
{ STXADD_OPER_MINUS 			  , "01000" },
{ STXADD_FUNC_UNKNOWN 			  , "01000" },
{ STXADD_PARAM_INVALID_COUNT	  , "01000" },
{ STXADD_JOIN_INVALID	  		  , "01000" },
{ STXADD_FROM_PAR_CLOSE	  		  , "01000" },
{ STXADD_KEYS_REQUIRED	  		  , "01000" },
{ STXADD_NOT_NUMBER	  		      , "01000" },
{ STXADD_COMMENT_OPEN			  , "01000" },
	};
}