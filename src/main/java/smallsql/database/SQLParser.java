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
 * SQLParser.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.database;

import java.util.List;
import java.sql.*;
import smallsql.database.language.Language;

final class SQLParser {

	SSConnection con;
	private char[] sql;
    private List tokens;
    private int tokenIdx;

    Command parse(SSConnection con, String sqlString) throws SQLException{
    	this.con = con;
        Command cmd = parse( sqlString.toCharArray() );
        SQLToken token = nextToken();
        if(token != null){
        	throw createSyntaxError(token, Language.STXADD_ADDITIONAL_TOK);
        }
        return cmd;
    }
    
    final private Command parse(char[] sql) throws SQLException{
        this.sql = sql;
        this.tokens = SQLTokenizer.parseSQL( sql );
        tokenIdx = 0;

        SQLToken token = nextToken(COMMANDS);
        switch (token.value){
            case SQLTokenizer.SELECT:
                    return select();
            case SQLTokenizer.DELETE:
                    return delete();
            case SQLTokenizer.INSERT:
                    return insert();
            case SQLTokenizer.UPDATE:
                    return update();
            case SQLTokenizer.CREATE:
                    return create();
            case SQLTokenizer.DROP:
                    return drop();
            case SQLTokenizer.ALTER:
                    return alter();
            case SQLTokenizer.SET:
                    return set();
			case SQLTokenizer.USE:
					token = nextToken(MISSING_EXPRESSION);
					String name = token.getName( sql );
					checkValidIdentifier( name, token );
					CommandSet set = new CommandSet( con.log, SQLTokenizer.USE);
					set.name = name;
					return set;
            case SQLTokenizer.EXECUTE:
                    return execute();
            case SQLTokenizer.TRUNCATE:
            		return truncate();
            default:
                    throw new Error();
        }
    }
    
    
    Expression parseExpression(String expr) throws SQLException{
		this.sql = expr.toCharArray();
		this.tokens = SQLTokenizer.parseSQL( sql );
		tokenIdx = 0;
    	return expression( null, 0);
    }

    /**
	 * Create a syntax error message, using a custom message.
	 * 
	 * @param token
	 *            token object; if not null, generates a SYNTAX_BASE_OFS,
	 *            otherwise a SYNTAX_BASE_END.
	 * @param addMessage
	 *            additional message object to append.
	 */
    private SQLException createSyntaxError(SQLToken token, String addMessageCode) {
    	String message = getErrorString(token, addMessageCode, null);
    	return SmallSQLException.create(Language.CUSTOM_MESSAGE, message);
    }
    
    /**
	 * Create a syntax error message, using a message with a parameter.
	 * 
	 * @param token
	 *            token object; if not null, generates a SYNTAX_BASE_OFS,
	 *            otherwise a SYNTAX_BASE_END.
	 * @param addMessageCode
	 *            additional message[Code] to append.
	 * @param param0
	 *            parameter.
	 */
    private SQLException createSyntaxError(SQLToken token, String addMessageCode, 
    		Object param0) {
    	String message = getErrorString(token, addMessageCode, param0);
    	return SmallSQLException.create(Language.CUSTOM_MESSAGE, message);
    }
    
    /**
	 * Create an "Additional keyword required" syntax error.
	 * 
	 * @param token
	 *            token object.
	 * @param validValues
	 *            valid values.
	 * @return Exception.
	 */
    private SQLException createSyntaxError(SQLToken token, int[] validValues){
    	String msgStr = SmallSQLException.translateMsg(
    			Language.STXADD_KEYS_REQUIRED, new Object[] { });
    	
    	StringBuffer msgBuf = new StringBuffer( msgStr );

        for(int i=0; i<validValues.length; i++){
            String name = SQLTokenizer.getKeyWord(validValues[i]);
            if(name == null) name = String.valueOf( (char)validValues[i] );
            msgBuf.append( name );
            if (i < validValues.length - 2)
                msgBuf.append( ", ");
            else
            if ( i == validValues.length - 2 )
                msgBuf.append( " or ");
        }

    	String message = getErrorString(
    			token, Language.CUSTOM_MESSAGE, msgBuf);
    	return SmallSQLException.create(Language.CUSTOM_MESSAGE, message);
    }

    /**
	 * Create the complete error string (begin + middle + end).
	 * 
	 * @param token
	 *            token object.
	 * @param middleMsgCode
	 *            middle message[code].
	 * @param middleMsgParam
	 *            middle message[code] parameter.
	 * @return complete error message string.
	 */
    private String getErrorString(SQLToken token, String middleMsgCode, 
    		Object middleMsgParam) {
    	StringBuffer buffer = new StringBuffer(1024);

    	/* begin */
    	
        if(token != null){
        	Object[] params = { String.valueOf(token.offset),
        						String.valueOf(sql, token.offset, token.length) };
        	String begin = SmallSQLException.translateMsg(Language.SYNTAX_BASE_OFS, params);
        	buffer.append(begin);
        }
        else{
        	String begin = SmallSQLException.translateMsg(
        			Language.SYNTAX_BASE_END, new Object[] { });
        	buffer.append(begin);
        }
    	
    	/* middle */
    	
    	String middle = SmallSQLException.translateMsg(
    			middleMsgCode, new Object[] { middleMsgParam });
    	
    	buffer.append(middle);
    	
    	/* end */
    	
        int valOffset = (token != null) ? token.offset : sql.length;
        int valBegin = Math.max( 0, valOffset-40);
        int valEnd   = Math.min( valOffset+20, sql.length );
        String lineSeparator = System.getProperty( "line.separator" );
        buffer.append( lineSeparator );
        buffer.append( sql, valBegin, valEnd-valBegin);
        buffer.append( lineSeparator );
        for(; valBegin<valOffset; valBegin++) buffer.append(' ');
        buffer.append('^');
    	
    	return buffer.toString();    	
    }
    
    private void checkValidIdentifier(String name, SQLToken token) throws SQLException{
        if(token.value == SQLTokenizer.ASTERISK) return;
        if(token.value != SQLTokenizer.VALUE &&
		   token.value != SQLTokenizer.IDENTIFIER &&
           token.value < 200){
            throw createSyntaxError( token, Language.STXADD_IDENT_EXPECT);
        }
        if(name.length() == 0) {
            throw createSyntaxError( token, Language.STXADD_IDENT_EMPTY, name);
        }
        char firstChar = name.charAt(0);
		if(firstChar != '#' && firstChar < '@') {
			throw createSyntaxError( token, Language.STXADD_IDENT_WRONG, name );
		}
    }
    
	/**
     * Returns a valid identifier from this token.
     * @param token the token of the identifier
     * @return the string with the name
     * @throws SQLException if the identifier is invalid
     */
    private String getIdentifier(SQLToken token) throws SQLException{
    	String name = token.getName(sql);
    	checkValidIdentifier( name, token );
    	return name;
    }
    
    
    /**
     * Returns a valid identifier from the next token from token stack.
     * @return the string with the name
     * @throws SQLException if the identifier is invalid
     */
    private String nextIdentifier() throws SQLException{
    	return getIdentifier( nextToken( MISSING_IDENTIFIER ) );
    }
    
    
    /**
     * Check if the identifier is a 2 part name with a point in the middle like FIRST.SECOND
     * @param name the name of the first part
     * @return the second part if exist else returns the first part
     * @throws SQLException 
     */
    private String nextIdentiferPart(String name) throws SQLException{
        SQLToken token = nextToken();
        //check if the object name include a database name
        if(token != null && token.value == SQLTokenizer.POINT){
            return nextIdentifier();
        }else{
            previousToken();
        }
        return name;
    }
    
    
    final private boolean isKeyword(SQLToken token){
    	if(token == null) return false;
    	switch(token.value){
    		case SQLTokenizer.SELECT:
    		case SQLTokenizer.INSERT:
    		case SQLTokenizer.UPDATE:
    		case SQLTokenizer.UNION:
    		case SQLTokenizer.FROM:
    		case SQLTokenizer.WHERE:
    		case SQLTokenizer.GROUP:
    		case SQLTokenizer.HAVING:
			case SQLTokenizer.ORDER:
    		case SQLTokenizer.COMMA:
			case SQLTokenizer.SET:
            case SQLTokenizer.JOIN:
            case SQLTokenizer.LIMIT:
    			return true;
    	}
    	return false;
    }
    
	/** 
	 * Return the last token that the method nextToken has return
	 */
	private SQLToken lastToken(){
		if(tokenIdx > tokens.size()){
			return null;
		}
		return (SQLToken)tokens.get( tokenIdx-1 );
	}
    private void previousToken(){
        tokenIdx--;
    }

    private SQLToken nextToken(){
        if(tokenIdx >= tokens.size()){
            tokenIdx++; // must be ever increment that the method previousToken() is working
            return null;
        }
        return (SQLToken)tokens.get( tokenIdx++ );
    }

    private SQLToken nextToken( int[] validValues) throws SQLException{
        SQLToken token = nextToken();
        if(token == null) throw createSyntaxError( token, validValues);
        if(validValues == MISSING_EXPRESSION){
            return token; // an expression can be contained in every token.
        }
        if(validValues == MISSING_IDENTIFIER){
            // the follow token are not valid identifier
            switch(token.value){
                case SQLTokenizer.PARENTHESIS_L:
                case SQLTokenizer.PARENTHESIS_R:
                case SQLTokenizer.COMMA:
                    throw createSyntaxError( token, validValues);
            }
            return token;
        }
        for(int i=validValues.length-1; i>=0; i--){
            if(token.value == validValues[i]) return token;
        }
        throw createSyntaxError( token, validValues);
    }
    

    /**
     * A single SELECT of a UNION or only a simple single SELECT.
     * @return
     * @throws SQLException
     */
    private CommandSelect singleSelect() throws SQLException{
        CommandSelect selCmd = new CommandSelect(con.log);
		SQLToken token;
        // scan for prefix like DISTINCT, ALL and the TOP clause; sample: SELECT TOP 15 ...
Switch: while(true){
			token = nextToken(MISSING_EXPRESSION);
			switch(token.value){
				case SQLTokenizer.TOP:
					token = nextToken(MISSING_EXPRESSION);
					try{
						int maxRows = Integer.parseInt(token.getName(sql));
						selCmd.setMaxRows(maxRows);
					}catch(NumberFormatException e){
						throw createSyntaxError(token, Language.STXADD_NOT_NUMBER, token.getName(sql));
					}
					break;
				case SQLTokenizer.ALL:
					selCmd.setDistinct(false);
					break;
				case SQLTokenizer.DISTINCT:
					selCmd.setDistinct(true);
					break;
				default:
					previousToken();
					break Switch;
			}
		}

        while(true){
            Expression column = expression(selCmd, 0);
            selCmd.addColumnExpression( column );

            token = nextToken();
            if(token == null) return selCmd; // SELECT without FROM

            boolean as = false;
            if(token.value == SQLTokenizer.AS){
                token = nextToken(MISSING_EXPRESSION);
                as = true;
            }

            if(as || (!isKeyword(token))){
            	String alias = getIdentifier( token);
                column.setAlias( alias );
                token = nextToken();
                if(token == null) return selCmd; // SELECT without FROM
            }

            switch(token.value){
                case SQLTokenizer.COMMA:
                        if(column == null) throw createSyntaxError( token, MISSING_EXPRESSION );
                        column = null;
                        break;
                case SQLTokenizer.FROM:
                        if(column == null) throw createSyntaxError( token, MISSING_EXPRESSION );
                        column = null;
                        from(selCmd);
                        return selCmd;

                default:
                        if(!isKeyword(token))
                			throw createSyntaxError( token, new int[]{SQLTokenizer.COMMA, SQLTokenizer.FROM} );
                        previousToken();
                        return selCmd;
            }
        }
    }
    
    
    final private CommandSelect select() throws SQLException{
		CommandSelect selCmd = singleSelect();
		SQLToken token = nextToken();
		   		
    	UnionAll union = null; 
	
		while(token != null && token.value == SQLTokenizer.UNION){
			if(union == null){
				union = new UnionAll();
				union.addDataSource(new ViewResult( con, selCmd ));
				selCmd = new CommandSelect(con.log);
				selCmd.setSource( union );
				DataSources from = new DataSources();
				from.add(union);
				selCmd.setTables( from );
				selCmd.addColumnExpression( new ExpressionName("*") );
			}
			nextToken(MISSING_ALL);
			nextToken(MISSING_SELECT);
			union.addDataSource( new ViewResult( con, singleSelect() ) );
			token = nextToken();
		}
		if(token != null && token.value == SQLTokenizer.ORDER){
			order( selCmd );
			token = nextToken();
		}
		if(token != null && token.value == SQLTokenizer.LIMIT){
            limit( selCmd );
            token = nextToken();
        }
        previousToken();
		return selCmd;
    }


    private Command delete() throws SQLException{
    	CommandDelete cmd = new CommandDelete(con.log);
    	nextToken(MISSING_FROM);
    	from(cmd);
		SQLToken token = nextToken();
		if(token != null){
			if(token.value != SQLTokenizer.WHERE)
				throw this.createSyntaxError(token, MISSING_WHERE);
			where(cmd);
		}
		return cmd;
    }


	private Command truncate() throws SQLException{
		CommandDelete cmd = new CommandDelete(con.log);
		nextToken(MISSING_TABLE);
		from(cmd);
		return cmd;
	}


    private Command insert() throws SQLException{
        SQLToken token = nextToken( MISSING_INTO );
        CommandInsert cmd = new CommandInsert( con.log, nextIdentifier() );

		int parthesisCount = 0;

		token = nextToken(MISSING_PARENTHESIS_VALUES_SELECT);
        if(token.value == SQLTokenizer.PARENTHESIS_L){
        	token = nextToken(MISSING_EXPRESSION);
        	if(token.value == SQLTokenizer.SELECT){
				parthesisCount++;
				cmd.noColumns = true;
        	}else{
				previousToken();
	            Expressions list = expressionParenthesisList(cmd);
	            for(int i=0; i<list.size(); i++){
	                cmd.addColumnExpression( list.get( i ) );
	            }
	            token = nextToken(MISSING_PARENTHESIS_VALUES_SELECT);
        	}
        }else cmd.noColumns = true;
        
Switch: while(true)
        switch(token.value){
        	case SQLTokenizer.VALUES:{
	            token = nextToken(MISSING_PARENTHESIS_L);
	            cmd.addValues( expressionParenthesisList(cmd) );
	            return cmd;
	        }
        	case SQLTokenizer.SELECT:
        		cmd.addValues( select() );
        		while(parthesisCount-- > 0){
        			nextToken(MISSING_PARENTHESIS_R);
        		}
        		return cmd;
        	case SQLTokenizer.PARENTHESIS_L:
        		token = nextToken(MISSING_PARENTHESIS_VALUES_SELECT);
        		parthesisCount++;
        		continue Switch;
        	default:
        		throw new Error();
        }
    }


    private Command update() throws SQLException{
		CommandUpdate cmd = new CommandUpdate(con.log);
		// read table name
		DataSources tables = new DataSources();
		cmd.setTables( tables );
		cmd.setSource( rowSource( cmd, tables, 0 ) );
		
		SQLToken token = nextToken(MISSING_SET);
		while(true){
			token = nextToken();
			Expression dest = expressionSingle( cmd, token);
			if(dest.getType() != Expression.NAME) throw createSyntaxError( token, MISSING_IDENTIFIER );
			nextToken(MISSING_EQUALS);
			Expression src = expression(cmd, 0);
			cmd.addSetting( dest, src);
			token = nextToken();
			if(token == null) break;
			switch(token.value){
				case SQLTokenizer.WHERE:
					where(cmd);
					return cmd;				
				case SQLTokenizer.COMMA:
					continue;
				default: throw createSyntaxError( token, MISSING_WHERE_COMMA );
			}
		}
		return cmd;
    }


    private Command create() throws SQLException{
        while(true){
            SQLToken token = nextToken(COMMANDS_CREATE);
            switch(token.value){
                case SQLTokenizer.DATABASE:
                    return createDatabase();
                case SQLTokenizer.TABLE:
                    return createTable();
                case SQLTokenizer.VIEW:
                    return createView();
                case SQLTokenizer.INDEX:
                    return createIndex(false);
                case SQLTokenizer.PROCEDURE:
                    return createProcedure();
                case SQLTokenizer.UNIQUE:
                    do{
                        token = nextToken(COMMANDS_CREATE_UNIQUE);
                    }while(token.value == SQLTokenizer.INDEX);
                    return createIndex(true);
                case SQLTokenizer.NONCLUSTERED:
                case SQLTokenizer.CLUSTERED:
                    continue;
                default:
                    throw createSyntaxError( token, COMMANDS_CREATE );
            }
        }
    }
	

    private CommandCreateDatabase createDatabase() throws SQLException{
        SQLToken token = nextToken();
        if(token == null) throw createSyntaxError( token, MISSING_EXPRESSION );
        return new CommandCreateDatabase( con.log, token.getName(sql));
    }
	
    
    private CommandTable createTable() throws SQLException{
        String catalog;
        String tableName = catalog = nextIdentifier();
        tableName = nextIdentiferPart(tableName);
        if(tableName == catalog) catalog = null;
        CommandTable cmdCreate = new CommandTable( con.log, catalog, tableName, SQLTokenizer.CREATE );
        SQLToken token = nextToken( MISSING_PARENTHESIS_L );

        nextCol:
        while(true){
            token = nextToken( MISSING_EXPRESSION );
			
			String constraintName;
            if(token.value == SQLTokenizer.CONSTRAINT){
            	// reading a CONSTRAINT with name
		    	constraintName = nextIdentifier();
				token = nextToken( MISSING_KEYTYPE );
            }else{
				constraintName = null;
            }
			switch(token.value){
				case SQLTokenizer.PRIMARY:
				case SQLTokenizer.UNIQUE:
				case SQLTokenizer.FOREIGN:
					IndexDescription index = index(cmdCreate, token.value, tableName, constraintName, null);
                    if(token.value == SQLTokenizer.FOREIGN){
                        nextToken( MISSING_REFERENCES );
                        String pk = nextIdentifier();
                        Expressions expressions = new Expressions();
                        Strings columns = new Strings();
                        expressionDefList( cmdCreate, expressions, columns );
                        IndexDescription pkIndex = new IndexDescription( null, pk, SQLTokenizer.UNIQUE, expressions, columns);
                        ForeignKey foreignKey = new ForeignKey(pk, pkIndex, tableName, index);
                        cmdCreate.addForeingnKey(foreignKey);
                    }else{
                        cmdCreate.addIndex( index );
                    }
	
					token = nextToken( MISSING_COMMA_PARENTHESIS );
					switch(token.value){
						case SQLTokenizer.PARENTHESIS_R:
							return cmdCreate;
						case SQLTokenizer.COMMA:
							continue nextCol;
					}
            }
            // the token is a column name
			token = addColumn( token, cmdCreate );
            if(token == null){
                throw createSyntaxError(token, MISSING_COMMA_PARENTHESIS);
            }
            switch(token.value){
                case SQLTokenizer.PARENTHESIS_R:
                    return cmdCreate;
                case SQLTokenizer.COMMA:
                    continue nextCol;
                default:
                    throw createSyntaxError(token, MISSING_COMMA_PARENTHESIS);
            }
        }
    }
    
	
    /**
     * Parse a Column and add it to the Command. If the column is unique or primary
     * then an index is added.
     * @param token the SQLToken with the column name
     * @return the token of the delimiter
     */
    private SQLToken addColumn(SQLToken token, CommandTable cmdCreate) throws SQLException{
        String colName = getIdentifier( token );
        Column col = datatype(false);
        col.setName( colName );

		token = nextToken();
        boolean nullableWasSet = false;
        boolean defaultWasSet = col.isAutoIncrement(); // with data type COUNTER already this value is set
        while(true){
            if(token == null){
                cmdCreate.addColumn( col );
                return null;
            }
            switch(token.value){
                case SQLTokenizer.PARENTHESIS_R:
                case SQLTokenizer.COMMA:
                    cmdCreate.addColumn( col );
                    return token;
                case SQLTokenizer.DEFAULT:
                    if(defaultWasSet) throw createSyntaxError( token, MISSING_COMMA_PARENTHESIS );
					int offset = token.offset + token.length;
                    token = nextToken();
                    if(token != null) offset = token.offset;
					previousToken();                    
					Expression expr = expression(cmdCreate, 0);
					SQLToken last = lastToken();
					int length = last.offset + last.length - offset;
					String def = new String( sql, offset, length );
                    col.setDefaultValue( expr, def );
                    defaultWasSet = true;
                    break;
                case SQLTokenizer.IDENTITY:
                    if(defaultWasSet) throw createSyntaxError( token, MISSING_COMMA_PARENTHESIS );
                    col.setAutoIncrement(true);
                    defaultWasSet = true;
                    break;
                case SQLTokenizer.NULL:
                    if(nullableWasSet) throw createSyntaxError( token, MISSING_COMMA_PARENTHESIS );
                    //col.setNullable(true); is already default
                    nullableWasSet = true;
                    break;
                case SQLTokenizer.NOT:
                    if(nullableWasSet) throw createSyntaxError( token, MISSING_COMMA_PARENTHESIS );
                    token = nextToken( MISSING_NULL );
                    col.setNullable(false);
                    nullableWasSet = true;
                    break;
				case SQLTokenizer.PRIMARY:
				case SQLTokenizer.UNIQUE:
					IndexDescription index = index(cmdCreate, token.value, cmdCreate.name, null, colName);
					cmdCreate.addIndex( index );
					break;
                default:
                    throw createSyntaxError(token, MISSING_OPTIONS_DATATYPE);
            }
            token = nextToken();
        }
    }
    

	/**
	 * Parse construct like:<br>
	 * <li>PRIMARY KEY (col1)
	 * <li>UNIQUE (col1, col2)
	 * <li>FOREIGN KEY REFERENCES ref_table(col1)
	 * @param cmd
	 * @param constraintType one of SQLTokenizer.PRIMARY, SQLTokenizer.UNIQUE or SQLTokenizer.FOREIGN.
	 * @param if it a constrain of the current column else null
	 * @return a new IndexDescription
	 */
	private IndexDescription index(Command cmd, int constraintType, String tableName, String contrainName, String columnName) throws SQLException{
		if(constraintType != SQLTokenizer.UNIQUE) nextToken( MISSING_KEY );
		SQLToken token = nextToken();
        if(token != null){
    		switch(token.value){
    			case SQLTokenizer.CLUSTERED:
    			case SQLTokenizer.NONCLUSTERED:
    				// ignoring, this tokens form MS SQL Server are ignored
    				break;
                default:
                    previousToken();
    		}
        }else{
            previousToken();
        }
		Strings columns = new Strings();
		Expressions expressions = new Expressions();
		if(columnName != null){
			//Constraint for a single column together with the column definition
			columns.add(columnName);
			expressions.add(new ExpressionName(columnName));
		}else{
			//Constraint as addition definition
            expressionDefList( cmd, expressions, columns );
		}
		return new IndexDescription( contrainName, tableName, constraintType, expressions, columns);
	}


    /**
     * Read a DataTpe description. This is used for CREATE TABLE and CONVERT function. 
     * @param isEscape true then the data types start with "SQL_". This is used for the Escape Syntax.
     */
    private Column datatype(boolean isEscape) throws SQLException{
		SQLToken token;
		int dataType;
		if(isEscape){
			token = nextToken( MISSING_SQL_DATATYPE );
			switch(token.value){
				case SQLTokenizer.SQL_BIGINT: 			dataType = SQLTokenizer.BIGINT;		break;
				case SQLTokenizer.SQL_BINARY:			dataType = SQLTokenizer.BINARY; 	break;
				case SQLTokenizer.SQL_BIT:				dataType = SQLTokenizer.BIT;		break;
				case SQLTokenizer.SQL_CHAR:				dataType = SQLTokenizer.CHAR;		break;
				case SQLTokenizer.SQL_DATE:				dataType = SQLTokenizer.DATE;		break;
				case SQLTokenizer.SQL_DECIMAL:			dataType = SQLTokenizer.DECIMAL;	break;
				case SQLTokenizer.SQL_DOUBLE:			dataType = SQLTokenizer.DOUBLE;		break;
				case SQLTokenizer.SQL_FLOAT:			dataType = SQLTokenizer.FLOAT;		break;
				case SQLTokenizer.SQL_INTEGER:			dataType = SQLTokenizer.INT;		break;
				case SQLTokenizer.SQL_LONGVARBINARY:	dataType = SQLTokenizer.LONGVARBINARY;break;
				case SQLTokenizer.SQL_LONGVARCHAR:		dataType = SQLTokenizer.LONGVARCHAR;break;
				case SQLTokenizer.SQL_REAL:				dataType = SQLTokenizer.REAL;		break;
				case SQLTokenizer.SQL_SMALLINT:			dataType = SQLTokenizer.SMALLINT;	break;
				case SQLTokenizer.SQL_TIME:				dataType = SQLTokenizer.TIME;		break;
				case SQLTokenizer.SQL_TIMESTAMP:		dataType = SQLTokenizer.TIMESTAMP;	break;
				case SQLTokenizer.SQL_TINYINT:			dataType = SQLTokenizer.TINYINT;	break;
				case SQLTokenizer.SQL_VARBINARY:		dataType = SQLTokenizer.VARBINARY;	break;
				case SQLTokenizer.SQL_VARCHAR:			dataType = SQLTokenizer.VARCHAR;	break;
				default: throw new Error();
			}
		}else{
			token = nextToken( MISSING_DATATYPE );
			dataType = token.value;
		}
		Column col = new Column();

		// two-part  data type
		if(dataType == SQLTokenizer.LONG){
			token = nextToken();
			if(token != null && token.value == SQLTokenizer.RAW){
				dataType = SQLTokenizer.LONGVARBINARY;
			}else{
				dataType = SQLTokenizer.LONGVARCHAR;
				previousToken();
			}
		}

		switch(dataType){
			case SQLTokenizer.RAW:
				dataType = SQLTokenizer.VARBINARY;
				// no break;
			case SQLTokenizer.CHAR:
			case SQLTokenizer.VARCHAR:
			case SQLTokenizer.NCHAR:
			case SQLTokenizer.NVARCHAR:
			case SQLTokenizer.BINARY:
			case SQLTokenizer.VARBINARY:
			{
				// detect the maximum column size
                token = nextToken();
				int displaySize;
				if(token == null || token.value != SQLTokenizer.PARENTHESIS_L){
					displaySize = 30;
                    previousToken();
				}else{
					token = nextToken( MISSING_EXPRESSION );
					try{
						displaySize = Integer.parseInt(token.getName(sql) );
					}catch(Exception e){
						throw createSyntaxError(token, MISSING_NUMBERVALUE );
					}
					nextToken( MISSING_PARENTHESIS_R );
				}
				col.setPrecision( displaySize );
				break;
			}
			case SQLTokenizer.SYSNAME:
				col.setPrecision(255);
				dataType = SQLTokenizer.VARCHAR;
				break;
			case SQLTokenizer.COUNTER:
				col.setAutoIncrement(true);
				dataType = SQLTokenizer.INT;
				break;
			case SQLTokenizer.NUMERIC:
			case SQLTokenizer.DECIMAL:
                token = nextToken();
				if(token != null && token.value == SQLTokenizer.PARENTHESIS_L){
					// read the precision of the data type
					token = nextToken( MISSING_EXPRESSION );
					int value;
					try{
						value = Integer.parseInt(token.getName(sql) );
					}catch(Exception e){
						throw createSyntaxError(token, MISSING_NUMBERVALUE );
					}
					col.setPrecision(value);
					token = nextToken( MISSING_COMMA_PARENTHESIS );
					if(token.value == SQLTokenizer.COMMA){
						// read the scale of the data type
						token = nextToken( MISSING_EXPRESSION );
						try{
							value = Integer.parseInt(token.getName(sql) );
						}catch(Exception e){
							throw createSyntaxError(token, MISSING_NUMBERVALUE );
						}
						col.setScale(value);
						nextToken( MISSING_PARENTHESIS_R );
					}
				}else{
					col.setPrecision(18); //default Precision for decimal and numeric
                    previousToken();
				}
				break;
		}
		col.setDataType( dataType );
		return col;
    }
    
    private CommandCreateView createView() throws SQLException{
    	String viewName = nextIdentifier();

		nextToken(MISSING_AS);
		SQLToken token = nextToken(MISSING_SELECT);
		CommandCreateView cmd = new CommandCreateView( con.log, viewName );
		
		cmd.sql = new String(sql, token.offset, sql.length-token.offset );
		select(); //Parse to check for valid
        return cmd;
    }


    private CommandTable createIndex(boolean unique) throws SQLException{
        String indexName = nextIdentifier();
        nextToken(MISSING_ON);
        String catalog;
        String tableName = catalog = nextIdentifier();
        tableName = nextIdentiferPart(tableName);
        if(tableName == catalog) catalog = null;
        CommandTable cmd = new CommandTable( con.log, catalog, tableName, SQLTokenizer.INDEX );
        Expressions expressions = new Expressions();
        Strings columns = new Strings();
        expressionDefList( cmd, expressions, columns );
        IndexDescription indexDesc = new IndexDescription( 
                indexName, 
                tableName, 
                unique ? SQLTokenizer.UNIQUE : SQLTokenizer.INDEX, 
                        expressions, 
                        columns);
        //TODO Create Index
		Object[] param = { "Create Index" };
        throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION, param);
    }

    private CommandCreateDatabase createProcedure() throws SQLException{
        //TODO Create Procedure
		Object[] param = { "Create Procedure" };
    	throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION, param);
    }

    private Command drop() throws SQLException{
        SQLToken tokenType = nextToken(COMMANDS_DROP);
        
		String catalog;
		String name = catalog = nextIdentifier();
        name = nextIdentiferPart( name );
        if(name == catalog) catalog = null;

        switch(tokenType.value){
            case SQLTokenizer.DATABASE:
            case SQLTokenizer.TABLE:
            case SQLTokenizer.VIEW:
            case SQLTokenizer.INDEX:
            case SQLTokenizer.PROCEDURE:
            	return new CommandDrop( con.log, catalog, name, tokenType.value);
            default:
                throw createSyntaxError( tokenType, COMMANDS_DROP );
        }
    }


    private Command alter() throws SQLException{
    	SQLToken tokenType = nextToken(COMMANDS_ALTER);
		String catalog;
		String tableName = catalog = nextIdentifier();
        switch(tokenType.value){
        case SQLTokenizer.TABLE:
        case SQLTokenizer.VIEW:
        case SQLTokenizer.INDEX:
        case SQLTokenizer.PROCEDURE:
            tableName = nextIdentiferPart(tableName);
            if(tableName == catalog) catalog = null;
        }
        switch(tokenType.value){
    	//case SQLTokenizer.DATABASE:
        case SQLTokenizer.TABLE:
            return alterTable( catalog, tableName );
        //case SQLTokenizer.VIEW:
        //case SQLTokenizer.INDEX:
        //case SQLTokenizer.PROCEDURE:
        default:
    		Object[] param = { "ALTER " + tokenType.getName( sql ) };
        	throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION, param);
        }
    }
    
    
    Command alterTable( String catalog, String name ) throws SQLException{
    	SQLToken tokenType = nextToken(MISSING_ADD_ALTER_DROP);
        CommandTable cmd = new CommandTable( con.log, catalog, name, tokenType.value );
    	switch(tokenType.value){
    	case SQLTokenizer.ADD:
    		SQLToken token;
    		do{
    			token = nextToken( MISSING_IDENTIFIER );
    			token = addColumn( token, cmd );
    		}while(token != null && token.value == SQLTokenizer.COMMA );

    		return cmd;
    	default:
    		Object[] param = { "ALTER TABLE " + tokenType.getName( sql ) };
            throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION, param);
    	}
    }
    

    private CommandSet set() throws SQLException{
        SQLToken token = nextToken( COMMANDS_SET );
        switch(token.value){
            case SQLTokenizer.TRANSACTION:
                return setTransaction();
            default:
                throw new Error();
        }
    }

    private CommandSet setTransaction() throws SQLException{
        SQLToken token = nextToken( MISSING_ISOLATION );
        token = nextToken( MISSING_LEVEL );
        token = nextToken( COMMANDS_TRANS_LEVEL );
        CommandSet cmd = new CommandSet( con.log, SQLTokenizer.LEVEL );
        switch(token.value){
            case SQLTokenizer.READ:
                token = nextToken( MISSING_COMM_UNCOMM );
                switch(token.value){
                    case SQLTokenizer.COMMITTED:
                        cmd.isolationLevel = Connection.TRANSACTION_READ_COMMITTED;
                        break;
                    case SQLTokenizer.UNCOMMITTED:
                        cmd.isolationLevel = Connection.TRANSACTION_READ_UNCOMMITTED;
                        break;
                    default:
                        throw new Error();
                }
                return cmd;
            case SQLTokenizer.REPEATABLE:
                token = nextToken( MISSING_READ );
                cmd.isolationLevel = Connection.TRANSACTION_REPEATABLE_READ;
                return cmd;
            case SQLTokenizer.SERIALIZABLE:
                cmd.isolationLevel = Connection.TRANSACTION_SERIALIZABLE;
                return cmd;
            default:
                throw new Error();
        }


    }

    private Command execute() throws SQLException{
        //TODO Execute
        throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION, "Execute");
    }

    /**
     * Read a Expression list in parenthesis like of VALUES() or functions. 
     * The left parenthesis is already consumed.
     * 
     * @param cmd is needed to add parameters "?" with addParameter() 
     * @see #expressionDefList
     */ 
    private Expressions expressionParenthesisList(Command cmd) throws SQLException{
		Expressions list = new Expressions();
		{
			SQLToken token = nextToken();
			if(token != null && token.value == SQLTokenizer.PARENTHESIS_R){
				// empty list like functions without parameters
				return list;
			}
			previousToken();
		}
        while(true){
            list.add( expression(cmd, 0) );
            SQLToken token = nextToken(MISSING_COMMA_PARENTHESIS);
            switch(token.value){
                case SQLTokenizer.PARENTHESIS_R:
                    return list;
                case SQLTokenizer.COMMA:
                    continue;
                default:
                    throw new Error();
            }
        }
    }

    
    /**
     * Read a list of expressions. The list is limit from specific SQL keywords like SELECT, GROUP BY, ORDER BY
     */
    private Expressions expressionTokenList(Command cmd, int listType) throws SQLException{
		Expressions list = new Expressions();
        while(true){
        	Expression expr = expression(cmd, 0);
            list.add( expr );
            SQLToken token = nextToken();
            
			if(listType == SQLTokenizer.ORDER && token != null){
				switch(token.value){
					case SQLTokenizer.DESC:
						expr.setAlias(SQLTokenizer.DESC_STR);
						//no break;
					case SQLTokenizer.ASC:
						token = nextToken();
				}				
			}
			
			if(token == null) {
				previousToken();
				return list;
			}

			switch(token.value){
                case SQLTokenizer.COMMA:
                    continue;
                default:
					if(isKeyword(token) ){
						previousToken();
						return list;
					}
                    throw createSyntaxError( token, MISSING_TOKEN_LIST);
            }
        }
    }
    
    
    private void expressionDefList(Command cmd, Expressions expressions, Strings columns) throws SQLException{
        SQLToken token = nextToken();
        if(token.value != SQLTokenizer.PARENTHESIS_L) throw createSyntaxError(token, MISSING_PARENTHESIS_L );
        Loop:
        while(true){
            int offset = token.offset + token.length;
            token = nextToken();
            if(token != null) offset = token.offset;
            previousToken();  
            
            expressions.add( expression(cmd, 0) );
            SQLToken last = lastToken();
            int length = last.offset + last.length - offset;
            columns.add( new String( sql, offset, length ) );

            token = nextToken(MISSING_COMMA_PARENTHESIS);
            switch(token.value){
                case SQLTokenizer.PARENTHESIS_R:
                    break Loop;
                case SQLTokenizer.COMMA:
                    continue;
                default:
                    throw new Error();
            }
        }
    }
    

	/**
	 * Read a complex expression that can be build from multiple atomic expressions.
     * @param cmd is needed to add parameters "?" with addParameter() 
	 * @param previousOperationLevel the level of the left operation.
	 */
    private Expression expression(Command cmd, int previousOperationLevel) throws SQLException{
        SQLToken token = nextToken(MISSING_EXPRESSION);
        Expression leftExpr;
        switch(token.value){
            case SQLTokenizer.NOT:
            	leftExpr =  new ExpressionArithmetic( expression( cmd, ExpressionArithmetic.NOT      / 10), ExpressionArithmetic.NOT);
            	break;
            case SQLTokenizer.MINUS:
            	leftExpr =  new ExpressionArithmetic( expression( cmd, ExpressionArithmetic.NEGATIVE / 10), ExpressionArithmetic.NEGATIVE);
            	break;
            case SQLTokenizer.TILDE:
            	leftExpr =  new ExpressionArithmetic( expression( cmd, ExpressionArithmetic.BIT_NOT  / 10), ExpressionArithmetic.BIT_NOT);
            	break;
            case SQLTokenizer.PARENTHESIS_L:
                leftExpr = expression( cmd, 0);
                token = nextToken(MISSING_PARENTHESIS_R);
                break;
            default:
                leftExpr = expressionSingle( cmd, token);
        }
        boolean isNot = false;
        while((token = nextToken()) != null){
            Expression rightExpr;
            int operation = ExpressionArithmetic.getOperationFromToken(token.value);
            int level = operation / 10;
            if(previousOperationLevel >= level){
                previousToken();
                return leftExpr;
            }
            switch(token.value){
                case SQLTokenizer.PLUS:
                case SQLTokenizer.MINUS:
                case SQLTokenizer.ASTERISK:
                case SQLTokenizer.SLACH:
                case SQLTokenizer.PERCENT:
                case SQLTokenizer.EQUALS:
                case SQLTokenizer.LESSER:
                case SQLTokenizer.LESSER_EQU:
                case SQLTokenizer.GREATER:
                case SQLTokenizer.GREATER_EQU:
                case SQLTokenizer.UNEQUALS:
                case SQLTokenizer.LIKE:
                case SQLTokenizer.OR:
                case SQLTokenizer.AND:
                case SQLTokenizer.BIT_AND:
                case SQLTokenizer.BIT_OR:
                case SQLTokenizer.BIT_XOR:
                    rightExpr = expression( cmd, level );
                    leftExpr = new ExpressionArithmetic( leftExpr, rightExpr, operation );
                    break;
                case SQLTokenizer.BETWEEN:
                    rightExpr = expression( cmd, ExpressionArithmetic.AND );
                    nextToken( MISSING_AND );
                    Expression rightExpr2 = expression( cmd, level );
                    leftExpr = new ExpressionArithmetic( leftExpr, rightExpr, rightExpr2, operation );
                    break;
                case SQLTokenizer.IN:
            		nextToken(MISSING_PARENTHESIS_L);
                	token = nextToken(MISSING_EXPRESSION);
                	if(token.value == SQLTokenizer.SELECT){
                		CommandSelect cmdSel = select();
						leftExpr = new ExpressionInSelect( con, leftExpr, cmdSel, operation );
						nextToken(MISSING_PARENTHESIS_R);
                	}else{
                		previousToken();
                		Expressions list = expressionParenthesisList( cmd );
                		leftExpr = new ExpressionArithmetic( leftExpr, list, operation );
                	}
                    break;
                case SQLTokenizer.IS:
                	token = nextToken(MISSING_NOT_NULL);
                	if(token.value == SQLTokenizer.NOT){
                		nextToken(MISSING_NULL);
						operation++;
                	}
                	leftExpr = new ExpressionArithmetic( leftExpr, operation );
                	break;
                case SQLTokenizer.NOT:
                	token = nextToken(MISSING_BETWEEN_IN);
                	previousToken();
                	isNot = true;
                	continue;
                default:
                        previousToken();
                        return leftExpr;
            }
            if(isNot){
            	isNot = false;
				leftExpr =  new ExpressionArithmetic( leftExpr, ExpressionArithmetic.NOT);
            }
        }
        previousToken();
        return leftExpr;
    }

    /**
     * This method parse a single expression like 12, 'qwert', 0x3F or a column name.
     * 
     * @param cmd is needed to add parameters "?" with addParameter() 
     */
    private Expression expressionSingle(Command cmd, SQLToken token) throws SQLException{
        boolean isMinus = false;
        if(token != null){
            switch(token.value){
                case SQLTokenizer.NULL:
                        return new ExpressionValue( null, SQLTokenizer.NULL );
                case SQLTokenizer.STRING:
                        return new ExpressionValue( token.getName(null), SQLTokenizer.VARCHAR );
                case SQLTokenizer.IDENTIFIER:
                        {
                        String name = getIdentifier( token );
                        ExpressionName expr =  new ExpressionName( name );
                        SQLToken token2 = nextToken();
                        if(token2 != null && token2.value == SQLTokenizer.POINT){
                            expr.setNameAfterTableAlias( nextIdentifier() );
                        }else{
                            previousToken();
                        }
                        return expr;
                        }
                case SQLTokenizer.TRUE:
                        return new ExpressionValue( Boolean.TRUE, SQLTokenizer.BOOLEAN );
                case SQLTokenizer.FALSE:
                        return new ExpressionValue( Boolean.FALSE, SQLTokenizer.BOOLEAN );
                case SQLTokenizer.ESCAPE_L:{
                        token = nextToken(COMMANDS_ESCAPE);
                        SQLToken para = nextToken(MISSING_EXPRESSION);
                        Expression expr;
                        switch(token.value){
                            case SQLTokenizer.D: // date escape sequence
                            	expr = new ExpressionValue( DateTime.valueOf(para.getName(sql), SQLTokenizer.DATE), SQLTokenizer.DATE );
                            	break;
                            case SQLTokenizer.T: // time escape sequence
                                expr = new ExpressionValue( DateTime.valueOf(para.getName(sql), SQLTokenizer.TIME), SQLTokenizer.TIME );
                            	break;
                            case SQLTokenizer.TS: // timestamp escape sequence
                                expr = new ExpressionValue( DateTime.valueOf(para.getName(sql), SQLTokenizer.TIMESTAMP), SQLTokenizer.TIMESTAMP );
                            	break;
                            case SQLTokenizer.FN: // function escape sequence
                            	nextToken(MISSING_PARENTHESIS_L);
                            	expr = function(cmd, para, true);
                            	break;
                            case SQLTokenizer.CALL: // call escape sequence
                                throw new java.lang.UnsupportedOperationException("call escape sequence");
                            default: throw new Error();
                        }
                        token = nextToken( ESCAPE_MISSING_CLOSE );
                        return expr;
                }
                case SQLTokenizer.QUESTION:
                        ExpressionValue param = new ExpressionValue();
                        cmd.addParameter( param );
                        return param;
                case SQLTokenizer.CASE:
                		return caseExpr(cmd);
                case SQLTokenizer.MINUS:
                case SQLTokenizer.PLUS:
                        // sign detection
                        do{
                            if(token.value == SQLTokenizer.MINUS)
                                    isMinus = !isMinus;
                            token = nextToken();
                            if(token == null) throw createSyntaxError( token, MISSING_EXPRESSION );
                        }while(token.value == SQLTokenizer.MINUS || token.value == SQLTokenizer.PLUS);
                        // no Break
                default:
                        SQLToken token2 = nextToken();
                        if(token2 != null && token2.value == SQLTokenizer.PARENTHESIS_L){
                            if(isMinus)
                                return new ExpressionArithmetic( function( cmd, token, false ),  ExpressionArithmetic.NEGATIVE );
                            return function( cmd, token, false );
                        }else{
                            // constant expression or identifier
                            char chr1 = sql[ token.offset ];
							if(chr1 == '$'){
								previousToken();
	                            String tok = new String(sql, token.offset+1, token.length-1);
                                if(isMinus) tok = "-" + tok;
								return new ExpressionValue( new Money(Double.parseDouble(tok)), SQLTokenizer.MONEY );
							}
                            String tok = new String(sql, token.offset, token.length);
                            if((chr1 >= '0' && '9' >= chr1) || chr1 == '.'){
                                previousToken();
                                // first character is a digit
                                if(token.length>1 && (sql[ token.offset +1 ] | 0x20) == 'x'){
                                    // binary data as hex
                                    if(isMinus) {
                						throw createSyntaxError(token, Language.STXADD_OPER_MINUS);
                                    }
                                    return new ExpressionValue( Utils.hex2bytes( sql, token.offset+2, token.length-2), SQLTokenizer.VARBINARY );
                                }
                                if(isMinus) tok = "-" + tok;
                                if(Utils.indexOf( '.', sql, token.offset, token.length ) >= 0 ||
                                   Utils.indexOf( 'e', sql, token.offset, token.length ) >= 0){
                                    return new ExpressionValue( new Double(tok), SQLTokenizer.DOUBLE );
                                }else{
                                    try{
                                        return new ExpressionValue( new Integer(tok), SQLTokenizer.INT );
                                    }catch(NumberFormatException e){
                                        return new ExpressionValue( new Long(tok), SQLTokenizer.BIGINT );
                                    }
                                }
                            }else{
                                // identifier
                                checkValidIdentifier( tok, token );
                                ExpressionName expr = new ExpressionName(tok);
                                if(token2 != null && token2.value == SQLTokenizer.POINT){
                                    expr.setNameAfterTableAlias( nextIdentifier() );
                                }else{
                                    previousToken();
                                }
                                if(isMinus)
                                    return new ExpressionArithmetic( expr,  ExpressionArithmetic.NEGATIVE );
                                return expr;
                            }
                        }
            }
        }
        return null;
    }
    
    
    ExpressionFunctionCase caseExpr(final Command cmd) throws SQLException{
		ExpressionFunctionCase expr = new ExpressionFunctionCase();
		SQLToken token = nextToken(MISSING_EXPRESSION);
		
		Expression input = null;
		if(token.value != SQLTokenizer.WHEN){
			// simple CASE Syntax
			previousToken();
			input = expression(cmd, 0);
			token = nextToken(MISSING_WHEN_ELSE_END);
		}			
			
		while(true){
			switch(token.value){
				case SQLTokenizer.WHEN:				
					Expression condition = expression(cmd, 0);
					if(input != null){
						// simple CASE Syntax
						condition = new ExpressionArithmetic( input, condition, ExpressionArithmetic.EQUALS);
					}
					nextToken(MISSING_THEN);
					Expression result = expression(cmd, 0);
					expr.addCase(condition, result);
					break;
				case SQLTokenizer.ELSE:
					expr.setElseResult(expression(cmd, 0));
					break;
				case SQLTokenizer.END:
					expr.setEnd();
					return expr;
				default:
					throw new Error();
			}
			token = nextToken(MISSING_WHEN_ELSE_END);
		}
    }
    

    /**
     * Parse any functions. The left parenthesis is already consumed from token list.
     * @param token the SQLToken of the function
     * @param isEscape If the function is a FN ESCAPE sequence
     */ 
    private Expression function( Command cmd, SQLToken token, boolean isEscape ) throws SQLException{
        Expression expr;
        switch(token.value){
        	case SQLTokenizer.CONVERT:{
        		Column col;
        		Expression style = null;
        		if(isEscape){
        			expr = expression( cmd, 0);
					nextToken(MISSING_COMMA);
					col = datatype(isEscape);
        		}else{
	        		col = datatype(isEscape);
	        		nextToken(MISSING_COMMA);
					expr = expression( cmd, 0);
					token = nextToken(MISSING_COMMA_PARENTHESIS);
					if(token.value == SQLTokenizer.COMMA){
						style = expression( cmd, 0);
					}else
						previousToken();
        		}
        		nextToken(MISSING_PARENTHESIS_R);
        		return new ExpressionFunctionConvert( col, expr, style );
        	}
        	case SQLTokenizer.CAST:
        		expr = expression( cmd, 0);
        		nextToken(MISSING_AS);
        		Column col = datatype(false);
        		nextToken(MISSING_PARENTHESIS_R);
        		return new ExpressionFunctionConvert( col, expr, null );
			case SQLTokenizer.TIMESTAMPDIFF:
				token = nextToken(MISSING_INTERVALS);
				nextToken(MISSING_COMMA);
				expr = expression( cmd, 0);
				nextToken(MISSING_COMMA);
				expr = new ExpressionFunctionTimestampDiff( token.value, expr, expression( cmd, 0));
				nextToken(MISSING_PARENTHESIS_R);
				return expr;
			case SQLTokenizer.TIMESTAMPADD:
				token = nextToken(MISSING_INTERVALS);
				nextToken(MISSING_COMMA);
				expr = expression( cmd, 0);
				nextToken(MISSING_COMMA);
				expr = new ExpressionFunctionTimestampAdd( token.value, expr, expression( cmd, 0));
				nextToken(MISSING_PARENTHESIS_R);
				return expr;
        }
		Expressions paramList = expressionParenthesisList(cmd);
        int paramCount = paramList.size();
        Expression[] params = paramList.toArray();
        boolean invalidParamCount;
        switch(token.value){
        // numeric functions:
            case SQLTokenizer.ABS:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionAbs();
                break;
            case SQLTokenizer.ACOS:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionACos();
                break;
            case SQLTokenizer.ASIN:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionASin();
                break;
            case SQLTokenizer.ATAN:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionATan();
                break;
            case SQLTokenizer.ATAN2:
                invalidParamCount = (paramCount != 2);
                expr = new ExpressionFunctionATan2();
                break;
            case SQLTokenizer.CEILING:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionCeiling();
                break;
            case SQLTokenizer.COS:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionCos();
                break;
            case SQLTokenizer.COT:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionCot();
                break;
            case SQLTokenizer.DEGREES:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionDegrees();
                break;
            case SQLTokenizer.EXP:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionExp();
                break;
            case SQLTokenizer.FLOOR:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionFloor();
                break;
            case SQLTokenizer.LOG:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionLog();
                break;
            case SQLTokenizer.LOG10:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionLog10();
                break;
            case SQLTokenizer.MOD:
                invalidParamCount = (paramCount != 2);
                expr = new ExpressionFunctionMod();
                break;
            case SQLTokenizer.PI:
                invalidParamCount = (paramCount != 0);
                expr = new ExpressionFunctionPI();
                break;
            case SQLTokenizer.POWER:
                invalidParamCount = (paramCount != 2);
                expr = new ExpressionFunctionPower();
                break;
            case SQLTokenizer.RADIANS:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionRadians();
                break;
            case SQLTokenizer.RAND:
                invalidParamCount =  (paramCount != 0) && (paramCount != 1);
                expr = new ExpressionFunctionRand();
                break;
            case SQLTokenizer.ROUND:
                invalidParamCount =  (paramCount != 2);
                expr = new ExpressionFunctionRound();
                break;
            case SQLTokenizer.SIN:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionSin();
                break;
            case SQLTokenizer.SIGN:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionSign();
                break;
            case SQLTokenizer.SQRT:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionSqrt();
                break;
            case SQLTokenizer.TAN:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionTan();
                break;
            case SQLTokenizer.TRUNCATE:
                invalidParamCount =  (paramCount != 2);
                expr = new ExpressionFunctionTruncate();
                break;
         
        // string functions:
			case SQLTokenizer.ASCII:
				invalidParamCount = (paramCount != 1);
				expr = new ExpressionFunctionAscii();
				break;
            case SQLTokenizer.BITLEN:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionBitLen();
                break;
            case SQLTokenizer.CHARLEN:
            case SQLTokenizer.CHARACTLEN:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionCharLen();
                break;
			case SQLTokenizer.CHAR:
				invalidParamCount = (paramCount != 1);
				expr = new ExpressionFunctionChar();
				break;
            case SQLTokenizer.CONCAT:
                if(paramCount != 2){
                    invalidParamCount = true;
                    expr = null;//only for compiler
                    break;
                }
                invalidParamCount = false;
                expr = new ExpressionArithmetic( params[0], params[1], ExpressionArithmetic.ADD);
                break;
            case SQLTokenizer.DIFFERENCE:
                invalidParamCount = (paramCount != 2);
                expr = new ExpressionFunctionDifference();
                break;
            case SQLTokenizer.INSERT:
                invalidParamCount = (paramCount != 4);
                expr = new ExpressionFunctionInsert();
                break;
            case SQLTokenizer.LCASE:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionLCase();
                break;
            case SQLTokenizer.LEFT:
                invalidParamCount = (paramCount != 2);
                expr = new ExpressionFunctionLeft();
                break;
			case SQLTokenizer.LENGTH:
				invalidParamCount = (paramCount != 1);
				expr = new ExpressionFunctionLength();
				break;
            case SQLTokenizer.LOCATE:
            	invalidParamCount = (paramCount != 2) && (paramCount != 3);
            	expr = new ExpressionFunctionLocate();
            	break;
            case SQLTokenizer.LTRIM:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionLTrim();
                break;
            case SQLTokenizer.OCTETLEN:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionOctetLen();
                break;
            case SQLTokenizer.REPEAT:
                invalidParamCount = (paramCount != 2);
                expr = new ExpressionFunctionRepeat();
                break;
            case SQLTokenizer.REPLACE:
                invalidParamCount = (paramCount != 3);
                expr = new ExpressionFunctionReplace();
                break;
			case SQLTokenizer.RIGHT:
				invalidParamCount = (paramCount != 2);
				expr = new ExpressionFunctionRight();
				break;
            case SQLTokenizer.RTRIM:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionRTrim();
                break;
            case SQLTokenizer.SPACE:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionSpace();
                break;
            case SQLTokenizer.SOUNDEX:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionSoundex();
                break;
			case SQLTokenizer.SUBSTRING:
				invalidParamCount = (paramCount != 3);
				expr = new ExpressionFunctionSubstring();
				break;
            case SQLTokenizer.UCASE:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionUCase();
                break;
                
        // date time functions
            case SQLTokenizer.CURDATE:
            case SQLTokenizer.CURRENTDATE:
            	invalidParamCount = (paramCount != 0);
				expr = new ExpressionValue( new DateTime(DateTime.now(), SQLTokenizer.DATE), SQLTokenizer.DATE);
				break;
            case SQLTokenizer.CURTIME:
            	invalidParamCount = (paramCount != 0);
				expr = new ExpressionValue( new DateTime(DateTime.now(), SQLTokenizer.TIME), SQLTokenizer.TIME);
				break;
            case SQLTokenizer.DAYOFMONTH:
            	invalidParamCount = (paramCount != 1);
				expr = new ExpressionFunctionDayOfMonth();
				break;
            case SQLTokenizer.DAYOFWEEK:
            	invalidParamCount = (paramCount != 1);
				expr = new ExpressionFunctionDayOfWeek();
				break;
            case SQLTokenizer.DAYOFYEAR:
            	invalidParamCount = (paramCount != 1);
				expr = new ExpressionFunctionDayOfYear();
				break;
            case SQLTokenizer.HOUR:
            	invalidParamCount = (paramCount != 1);
				expr = new ExpressionFunctionHour();
				break;
            case SQLTokenizer.MINUTE:
            	invalidParamCount = (paramCount != 1);
				expr = new ExpressionFunctionMinute();
				break;
            case SQLTokenizer.MONTH:
            	invalidParamCount = (paramCount != 1);
				expr = new ExpressionFunctionMonth();
				break;
            case SQLTokenizer.NOW:
            	invalidParamCount = (paramCount != 0);
				expr = new ExpressionValue( new DateTime(DateTime.now(), SQLTokenizer.TIMESTAMP), SQLTokenizer.TIMESTAMP);
				break;
            case SQLTokenizer.YEAR:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionYear();
                break;
            	
        // system functions:
            case SQLTokenizer.IIF:
        		invalidParamCount = (paramCount != 3);
            	expr = new ExpressionFunctionIIF();
        		break;
        	case SQLTokenizer.SWITCH:
        		invalidParamCount = (paramCount % 2 != 0);
        		ExpressionFunctionCase exprCase = new ExpressionFunctionCase();
        		for(int i=0; i < paramCount-1; i +=2)
        			exprCase.addCase(params[i], params[i+1] );
        		exprCase.setEnd();
        		expr = exprCase;
        		break;
        	case SQLTokenizer.IFNULL:
        		switch(paramCount){
        			case 1:
        				return new ExpressionArithmetic( params[0], ExpressionArithmetic.ISNULL );
        			case 2:        				
        				invalidParamCount = false;
        				expr = new ExpressionFunctionIIF();
        				Expression[] newParams = new Expression[3];
        				newParams[0] = new ExpressionArithmetic( params[0], ExpressionArithmetic.ISNULL );
        				newParams[1] = params[1];
        				newParams[2] = params[0];        				
        				params = newParams;
        				paramCount = 3;
        				break;
        			default:
        				invalidParamCount = true;
        				expr = null; // only for Compiler
        		}
        		break;
                    
        // now come the aggregate functions
            case SQLTokenizer.COUNT:
					invalidParamCount = (paramCount != 1);
					if(params[0].getType() == Expression.NAME){
						//detect special case COUNT(*)
						ExpressionName param = (ExpressionName)params[0];
						if("*".equals(param.getName()) && param.getTableAlias() == null){
                            //set any not NULL value as parameter
							params[0] = new ExpressionValue("*", SQLTokenizer.VARCHAR);
						}
					}
					expr = new ExpressionName( Expression.COUNT );
					break;
			case SQLTokenizer.SUM:
					invalidParamCount = (paramCount != 1);
					expr = new ExpressionName( Expression.SUM );
					break;
			case SQLTokenizer.MAX:
					invalidParamCount = (paramCount != 1);
					expr = new ExpressionName( Expression.MAX );
					break;
			case SQLTokenizer.MIN:
					invalidParamCount = (paramCount != 1);
					expr = new ExpressionName( Expression.MIN );
					break;
			case SQLTokenizer.FIRST:
					invalidParamCount = (paramCount != 1);
					expr = new ExpressionName( Expression.FIRST );
					break;
			case SQLTokenizer.LAST:
					invalidParamCount = (paramCount != 1);
					expr = new ExpressionName( Expression.LAST );
					break;
			case SQLTokenizer.AVG:
					if(paramCount != 1){
                        invalidParamCount = true;
                        expr = null;//Only for the compiler
                        break;
                    }
					expr = new ExpressionName( Expression.SUM );
					expr.setParams( params );
					Expression expr2 = new ExpressionName( Expression.COUNT );
					expr2.setParams( params );
					expr = new ExpressionArithmetic( expr, expr2, ExpressionArithmetic.DIV );
					return expr;
            default:
            	throw createSyntaxError(token, Language.STXADD_FUNC_UNKNOWN);
        }
        if(invalidParamCount) {
        	throw createSyntaxError(token, Language.STXADD_PARAM_INVALID_COUNT);
        }
        expr.setParams( params );
        return expr;
    }

    /**
     * read a table or view name in a FROM clause. If the keyword AS exists then read it also the alias
     */
    private RowSource tableSource( Command cmd, DataSources tables) throws SQLException{
        SQLToken token = nextToken(MISSING_EXPRESSION);
        switch(token.value){
            case SQLTokenizer.PARENTHESIS_L: // (
                    return rowSource( cmd, tables, SQLTokenizer.PARENTHESIS_R );
            case SQLTokenizer.ESCAPE_L: // {
                    token = nextToken(MISSING_OJ);
                    return rowSource( cmd, tables, SQLTokenizer.ESCAPE_R );
            case SQLTokenizer.SELECT:
            		// inner select
            		ViewResult viewResult = new ViewResult( con, select() );
            		tables.add(viewResult);
            		return viewResult;
        }
        String catalog = null;
        String name = getIdentifier( token );
		token = nextToken();
		//check if the table name include a database name
		if(token != null && token.value == SQLTokenizer.POINT){
			catalog = name;
			name = nextIdentifier();
			token = nextToken();
		}
		//TableResult table = new TableResult();
		//table.setName( catalog, name );
		TableView tableView = Database.getTableView( con, catalog, name);
		TableViewResult table = TableViewResult.createResult(tableView);
        tables.add( table );

        if(token != null && token.value == SQLTokenizer.AS){
            // skip AS keyword, if exists
            token = nextToken(MISSING_EXPRESSION);
            table.setAlias( token.getName( sql ) );
        }else{
            previousToken();
        }
        return table;
    }
    

    /**
     * read a join in a from clause.
     */
    private Join join(Command cmd, DataSources tables, RowSource left, int type) throws SQLException{
        RowSource right = rowSource(cmd, tables, 0);
        SQLToken token = nextToken();

        while(true){
            if(token == null) {
            	throw createSyntaxError(token, Language.STXADD_JOIN_INVALID);
            }

            switch(token.value){
            	case SQLTokenizer.ON:
	            	if(type == Join.RIGHT_JOIN)
						return new Join( Join.LEFT_JOIN, right, left, expression( cmd, 0 ) );
	                return new Join( type, left, right, expression( cmd, 0 ) );
	            default:
	                if(!right.hasAlias()){
	                    right.setAlias( token.getName( sql ) );
	                    token = nextToken();
	                    continue;
	                }
	                throw createSyntaxError( token, MISSING_ON );	                
            }
        }
    }

    /**
     * returns a row source. A row source is a Table, Join, View or a row function.
     *
     */
    private RowSource rowSource(Command cmd, DataSources tables, int parenthesis) throws SQLException{
        RowSource fromSource = null;
        fromSource = tableSource(cmd, tables);

        while(true){
            SQLToken token = nextToken();
            if(token == null) return fromSource;
            switch(token.value){
                case SQLTokenizer.ON:
                    previousToken();
                    return fromSource;
                case SQLTokenizer.CROSS:
                    nextToken(MISSING_JOIN);
                    //no break
                case SQLTokenizer.COMMA:
                    fromSource = new Join( Join.CROSS_JOIN, fromSource, rowSource(cmd, tables, 0), null);
                    break;
                case SQLTokenizer.INNER:
                    nextToken(MISSING_JOIN);
                    //no break;
                case SQLTokenizer.JOIN:
                    fromSource = join( cmd, tables, fromSource, Join.INNER_JOIN );
                    break;
                case SQLTokenizer.LEFT:
                    token = nextToken(MISSING_OUTER_JOIN);
                	if(token.value == SQLTokenizer.OUTER)
                		token = nextToken(MISSING_JOIN);
                    fromSource = join( cmd, tables, fromSource, Join.LEFT_JOIN );
                    break;
                case SQLTokenizer.RIGHT:
                	token = nextToken(MISSING_OUTER_JOIN);
                	if(token.value == SQLTokenizer.OUTER)
                		token = nextToken(MISSING_JOIN);
					fromSource = join( cmd, tables, fromSource, Join.RIGHT_JOIN );
					break;                	
				case SQLTokenizer.FULL:
					token = nextToken(MISSING_OUTER_JOIN);
					if(token.value == SQLTokenizer.OUTER)
						token = nextToken(MISSING_JOIN);
					fromSource = join( cmd, tables, fromSource, Join.FULL_JOIN );
					break;                	
                case SQLTokenizer.PARENTHESIS_R:
                case SQLTokenizer.ESCAPE_R:
                    if(parenthesis == token.value) return fromSource;
                    if(parenthesis == 0){
                    	previousToken();
						return fromSource;
                    }
                    throw createSyntaxError( token, Language.STXADD_FROM_PAR_CLOSE );
                default:
                	if(isKeyword(token)){
						previousToken();
						return fromSource;
                	}
                    if(!fromSource.hasAlias()){
                        fromSource.setAlias( token.getName( sql ) );
                        break;
                    }
                    throw createSyntaxError( token, new int[]{SQLTokenizer.COMMA, SQLTokenizer.GROUP, SQLTokenizer.ORDER, SQLTokenizer.HAVING} );
            }
        }
    }

    private void from(CommandSelect cmd) throws SQLException{
		DataSources tables = new DataSources();
        cmd.setTables(tables);
        cmd.setSource( rowSource( cmd, tables, 0 ) );

		SQLToken token;
        while(null != (token = nextToken())){
            switch(token.value){
                case SQLTokenizer.WHERE:
                    where( cmd );
                    break;
                case SQLTokenizer.GROUP:
                    group( cmd );
                    break;
                case SQLTokenizer.HAVING:
                    having( cmd );
                    break;
                default:
                	previousToken();
                    return;
            }
        }
    }

    private void order(CommandSelect cmd) throws SQLException{
        nextToken(MISSING_BY);
        cmd.setOrder(expressionTokenList(cmd, SQLTokenizer.ORDER));
    }
    
    private void limit(CommandSelect selCmd) throws SQLException{
        SQLToken token = nextToken(MISSING_EXPRESSION);
        try{
            int maxRows = Integer.parseInt(token.getName(sql));
            selCmd.setMaxRows(maxRows);
        }catch(NumberFormatException e){
            throw createSyntaxError(token, Language.STXADD_NOT_NUMBER, token.getName(sql));
        }
    }

    private void group(CommandSelect cmd) throws SQLException{
        nextToken(MISSING_BY);
        cmd.setGroup( expressionTokenList(cmd, SQLTokenizer.GROUP) );
    }

    private void where(CommandSelect cmd) throws SQLException{
        cmd.setWhere( expression(cmd, 0) );
    }

    private void having(CommandSelect cmd) throws SQLException{
        cmd.setHaving( expression(cmd, 0) );
    }


    private static final int[] COMMANDS = {SQLTokenizer.SELECT, SQLTokenizer.DELETE, SQLTokenizer.INSERT, SQLTokenizer.UPDATE, SQLTokenizer.CREATE, SQLTokenizer.DROP, SQLTokenizer.ALTER, SQLTokenizer.SET, SQLTokenizer.USE, SQLTokenizer.EXECUTE, SQLTokenizer.TRUNCATE};
    private static final int[] COMMANDS_ESCAPE = {SQLTokenizer.D, SQLTokenizer.T, SQLTokenizer.TS, SQLTokenizer.FN, SQLTokenizer.CALL};
    private static final int[] COMMANDS_ALTER = {SQLTokenizer.DATABASE, SQLTokenizer.TABLE, SQLTokenizer.VIEW,  SQLTokenizer.PROCEDURE, };
    private static final int[] COMMANDS_CREATE = {SQLTokenizer.DATABASE, SQLTokenizer.TABLE, SQLTokenizer.VIEW, SQLTokenizer.INDEX, SQLTokenizer.PROCEDURE, SQLTokenizer.UNIQUE, SQLTokenizer.CLUSTERED, SQLTokenizer.NONCLUSTERED};
    private static final int[] COMMANDS_DROP = {SQLTokenizer.DATABASE, SQLTokenizer.TABLE, SQLTokenizer.VIEW, SQLTokenizer.INDEX, SQLTokenizer.PROCEDURE};
    private static final int[] COMMANDS_SET = {SQLTokenizer.TRANSACTION};
    private static final int[] COMMANDS_CREATE_UNIQUE = {SQLTokenizer.INDEX, SQLTokenizer.CLUSTERED, SQLTokenizer.NONCLUSTERED};
	private static final int[] MISSING_TABLE = {SQLTokenizer.TABLE};
    private static final int[] ESCAPE_MISSING_CLOSE = {SQLTokenizer.ESCAPE_R};
    private static final int[] MISSING_EXPRESSION = {SQLTokenizer.VALUE};
    private static final int[] MISSING_IDENTIFIER = {SQLTokenizer.IDENTIFIER};
    private static final int[] MISSING_BY = {SQLTokenizer.BY};
    private static final int[] MISSING_PARENTHESIS_L = {SQLTokenizer.PARENTHESIS_L};
    private static final int[] MISSING_PARENTHESIS_R = {SQLTokenizer.PARENTHESIS_R};
    private static final int[] MISSING_DATATYPE  = {SQLTokenizer.BIT, SQLTokenizer.BOOLEAN, SQLTokenizer.BINARY, SQLTokenizer.VARBINARY, SQLTokenizer.RAW, SQLTokenizer.LONGVARBINARY, SQLTokenizer.BLOB, SQLTokenizer.TINYINT, SQLTokenizer.SMALLINT, SQLTokenizer.INT, SQLTokenizer.COUNTER, SQLTokenizer. BIGINT, SQLTokenizer.SMALLMONEY, SQLTokenizer.MONEY, SQLTokenizer.DECIMAL, SQLTokenizer.NUMERIC, SQLTokenizer.REAL, SQLTokenizer.FLOAT, SQLTokenizer.DOUBLE, SQLTokenizer.DATE, SQLTokenizer.TIME, SQLTokenizer.TIMESTAMP, SQLTokenizer.SMALLDATETIME, SQLTokenizer.CHAR, SQLTokenizer.NCHAR, SQLTokenizer.VARCHAR, SQLTokenizer.NVARCHAR, SQLTokenizer.LONG, SQLTokenizer.LONGNVARCHAR, SQLTokenizer.LONGVARCHAR, SQLTokenizer.CLOB, SQLTokenizer.NCLOB, SQLTokenizer.UNIQUEIDENTIFIER, SQLTokenizer.JAVA_OBJECT, SQLTokenizer.SYSNAME};
	private static final int[] MISSING_SQL_DATATYPE = { SQLTokenizer.SQL_BIGINT , SQLTokenizer.SQL_BINARY , SQLTokenizer.SQL_BIT , SQLTokenizer.SQL_CHAR , SQLTokenizer.SQL_DATE , SQLTokenizer.SQL_DECIMAL , SQLTokenizer.SQL_DOUBLE , SQLTokenizer.SQL_FLOAT , SQLTokenizer.SQL_INTEGER , SQLTokenizer.SQL_LONGVARBINARY , SQLTokenizer.SQL_LONGVARCHAR , SQLTokenizer.SQL_REAL , SQLTokenizer.SQL_SMALLINT , SQLTokenizer.SQL_TIME , SQLTokenizer.SQL_TIMESTAMP , SQLTokenizer.SQL_TINYINT , SQLTokenizer.SQL_VARBINARY , SQLTokenizer.SQL_VARCHAR };
    private static final int[] MISSING_INTO = {SQLTokenizer.INTO};
	private static final int[] MISSING_BETWEEN_IN = {SQLTokenizer.BETWEEN, SQLTokenizer.IN};
	private static final int[] MISSING_NOT_NULL = {SQLTokenizer.NOT, SQLTokenizer.NULL};
    private static final int[] MISSING_NULL = {SQLTokenizer.NULL};
	private static final int[] MISSING_COMMA = {SQLTokenizer.COMMA};
    private static final int[] MISSING_COMMA_PARENTHESIS = {SQLTokenizer.COMMA, SQLTokenizer.PARENTHESIS_R};
    private static final int[] MISSING_PARENTHESIS_VALUES_SELECT = {SQLTokenizer.PARENTHESIS_L, SQLTokenizer.VALUES, SQLTokenizer.SELECT};
    private static final int[] MISSING_TOKEN_LIST = {SQLTokenizer.COMMA, SQLTokenizer.FROM, SQLTokenizer.GROUP, SQLTokenizer.HAVING, SQLTokenizer.ORDER};
	private static final int[] MISSING_FROM = {SQLTokenizer.FROM};
	private static final int[] MISSING_SET = {SQLTokenizer.SET};
	private static final int[] MISSING_EQUALS = {SQLTokenizer.EQUALS};
	private static final int[] MISSING_WHERE = {SQLTokenizer.WHERE};
	private static final int[] MISSING_WHERE_COMMA = {SQLTokenizer.WHERE, SQLTokenizer.COMMA};
    private static final int[] MISSING_ISOLATION = {SQLTokenizer.ISOLATION};
    private static final int[] MISSING_LEVEL = {SQLTokenizer.LEVEL};
    private static final int[] COMMANDS_TRANS_LEVEL = {SQLTokenizer.READ, SQLTokenizer.REPEATABLE, SQLTokenizer.SERIALIZABLE};
    private static final int[] MISSING_READ = {SQLTokenizer.READ};
    private static final int[] MISSING_COMM_UNCOMM = {SQLTokenizer.COMMITTED, SQLTokenizer.UNCOMMITTED};
    private static final int[] MISSING_OPTIONS_DATATYPE = { SQLTokenizer.DEFAULT, SQLTokenizer.IDENTITY, SQLTokenizer.NOT, SQLTokenizer.NULL, SQLTokenizer.PRIMARY, SQLTokenizer.UNIQUE, SQLTokenizer.COMMA, SQLTokenizer.PARENTHESIS_R};
    private static final int[] MISSING_NUMBERVALUE = {SQLTokenizer.NUMBERVALUE};
    private static final int[] MISSING_AND = {SQLTokenizer.AND};
    private static final int[] MISSING_JOIN = {SQLTokenizer.JOIN};
    private static final int[] MISSING_OUTER_JOIN = {SQLTokenizer.OUTER, SQLTokenizer.JOIN};
    private static final int[] MISSING_OJ = {SQLTokenizer.OJ};
    private static final int[] MISSING_ON = {SQLTokenizer.ON};
	private static final int[] MISSING_KEYTYPE = {SQLTokenizer.PRIMARY, SQLTokenizer.UNIQUE, SQLTokenizer.FOREIGN};
	private static final int[] MISSING_KEY = {SQLTokenizer.KEY};
    private static final int[] MISSING_REFERENCES = {SQLTokenizer.REFERENCES};
	private static final int[] MISSING_AS = {SQLTokenizer.AS};
	private static final int[] MISSING_SELECT = {SQLTokenizer.SELECT};
	private static final int[] MISSING_INTERVALS = {SQLTokenizer.SQL_TSI_FRAC_SECOND, SQLTokenizer.SQL_TSI_SECOND, SQLTokenizer.SQL_TSI_MINUTE, SQLTokenizer.SQL_TSI_HOUR, SQLTokenizer.SQL_TSI_DAY, SQLTokenizer.SQL_TSI_WEEK, SQLTokenizer.SQL_TSI_MONTH, SQLTokenizer.SQL_TSI_QUARTER, SQLTokenizer.SQL_TSI_YEAR, SQLTokenizer.MILLISECOND, SQLTokenizer.SECOND, SQLTokenizer.MINUTE, SQLTokenizer.HOUR, SQLTokenizer.DAY, SQLTokenizer.WEEK, SQLTokenizer.MONTH, SQLTokenizer.QUARTER, SQLTokenizer.YEAR, SQLTokenizer.D};
	private static final int[] MISSING_ALL = {SQLTokenizer.ALL};
	private static final int[] MISSING_THEN = {SQLTokenizer.THEN};
	private static final int[] MISSING_WHEN_ELSE_END = {SQLTokenizer.WHEN, SQLTokenizer.ELSE, SQLTokenizer.END};
	private static final int[] MISSING_ADD_ALTER_DROP = {SQLTokenizer.ADD, SQLTokenizer.ALTER, SQLTokenizer.DROP};
	
	
}