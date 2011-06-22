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
 * SQLTokenizer.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.database;

import java.util.*;
import java.sql.SQLException;
import java.sql.Types;

import smallsql.database.language.Language;

public class SQLTokenizer {
	private static final int NOT_COMMENT = 0;
	private static final int LINE_COMMENT = 1;
	private static final int MULTI_COMMENT = 2;

    public static List parseSQL( char[] sql ) throws SQLException{
        SearchNode node = searchTree;
        ArrayList tokens = new ArrayList();
        int value = 0;
        int tokenStart = 0;
        boolean wasWhiteSpace = true;
        int comment = NOT_COMMENT;
        char quote = 0;
        StringBuffer quoteBuffer = new StringBuffer();

        for(int i=0; i<sql.length; i++){
            char c = sql[i];
            switch(c){
                case '\"':
                case '\'':
                			if (comment != NOT_COMMENT) {
                				break;
                			}else if(quote == 0){
                                quote = c;
                            }else if(quote == c){
                                // check on escaped quote
                                if(i+1<sql.length && sql[i+1] == quote){
                                    quoteBuffer.append(quote);
                                    i++;
                                }else{
                                    tokens.add( new SQLToken( quoteBuffer.toString(), (quote == '\'') ? STRING : IDENTIFIER,       tokenStart, i+1) );
                                    quoteBuffer.setLength(0);
                                    quote = 0;
                                    tokenStart = i+1;
                                    wasWhiteSpace = true;
                                }
                            }else quoteBuffer.append(c);
                            break;
                case '.':
		        			if (comment != NOT_COMMENT) {
		        				break;
		        			}else if(quote == 0){
                            	// there are follow cases with a point
                            	// "abc"."abc"	--> identifier --> multiple tokens
                            	// "5"."3"		--> identifier --> multiple tokens
                            	// 5.3			--> number --> one token
                            	// 5.e3			--> number --> one token
                            	// .3			--> number --> one token
                            	// .e3			--> identifier --> multiple tokens
                                int k=tokenStart;
                                if(k == i){ // point is first character
                                	if(sql.length> k+1){
										char cc = sql[k+1];
										if((cc >= '0') && cc <= '9') break; // is a number --> break
                                	}
                                }else{
	                                for(; k<i; k++){
	                                    char cc = sql[k];
	                                    if((cc != '-' && cc != '$' && cc < '0') || cc > '9') break; // is identifier --> break
	                                }
									if(k>=i) break; // preceding tokens are only digits that it is not an identifier else a floating number
                                }
                            }
                            // character before is not a digit that it is an identifier
                            // no break;
				case '-':
							if (comment != NOT_COMMENT) {
								break;
							}
							/* start of single line comment */
							else if (c == '-' && (i+1 < sql.length) && (sql[i+1] == '-')) {
	                            if(!wasWhiteSpace){
	                                tokens.add( new SQLToken( value, tokenStart, i) );
	                                value = 0;
	                            }
								i++;
								tokenStart = i+1;
								comment = LINE_COMMENT;
							}
							else if(quote == 0 && !wasWhiteSpace){
								char c1 = sql[tokenStart];
								char cx = sql[i-1];
								if(((c1 >= '0' && c1 <= '9') || c1 == '.') && (cx == 'e' || cx == 'E'))
									//negative exponential number
									break;
								if(c1 == '$' && tokenStart+1 == i)
									// money number
									break;
							}
                case ' ':
				case '\t':
				case '\n':
				case '\r':
                case ',':
                case '(':
                case ')':
                case '{':
                case '}':
                case '*':
                case '+':
                case '/':
                case '%':
                case '&':
                case '|':
                case '=':
                case '<':
                case '>':
                case '?':
                case '^':
                case '~':
                			/* end of line comment */
                			if (comment == LINE_COMMENT) {
                				// '\r'/'\n' check needed because of fall-through
                				if (c == '\r' || c == '\n') {
                					comment = NOT_COMMENT;
                                    wasWhiteSpace = true;
                				}
                                tokenStart = i+1;
                				break;
                			}
                			/* end of multi-line comment */
                			else if (comment == MULTI_COMMENT) {
                				// '*' check needed because of fall-through
                				if (c == '*' && (i+1 < sql.length) && (sql[i+1] == '/')) {
                					comment = NOT_COMMENT;
                                    wasWhiteSpace = true;
                                    i++;
                				}
                                tokenStart = i + 1;
                				break;
                			}
                			else if(quote == 0){
                                if(!wasWhiteSpace){
                                   tokens.add( new SQLToken( value, tokenStart, i) );
                                   value = 0;
                                }
                                switch(c){
									case ' ':
                                	case '\t':
                                	case '\n':
                                	case '\r':
                                    	// skip this characters, this are not tokens, this are only source formatter   
                                    	break;
                                    case '<':
                                        if((i+1 < sql.length) && (sql[i+1] == '>')){
                                            tokens.add( new SQLToken( UNEQUALS, i, i+2) );
                                            i++;
                                            break;
                                        }
                                    case '>':
                                        if((i+1 < sql.length) && (sql[i+1] == '=')){
                                            tokens.add( new SQLToken( 100 + c, i, i+2) );
                                            i++;
                                            break;
                                        }
                                    /* start of multi-line comment */
                                    case '/':
                                        if((i+1 < sql.length) && (sql[i+1] == '*')){
            								i++;
            								tokenStart = i+1;
            								comment = MULTI_COMMENT;
                                            break;
                                        }
                                    default:
                                        tokens.add( new SQLToken( c, i, i+1) );
                                }
                                wasWhiteSpace = true;
                                tokenStart = i+1;
                            }else{
                                quoteBuffer.append(c);
                            }
                            break;
                default:
                			if (comment != NOT_COMMENT) {
                				break;
                			}else if(quote == 0){
                                if(wasWhiteSpace){
                                     node = searchTree;
                                }else{
                                    if(node == null){
                                        value = 0;
                                        wasWhiteSpace = false;
                                        break;
                                    }
                                }
								c |= 0x20; // case insensitive
                                while(node != null && node.letter != c) node = node.nextEntry;
                                if(node != null){
                                    value = node.value;
                                    node = node.nextLetter;
                                }else{
                                    value = 0;
                                    node = null;
                                }
                            }else{
                                quoteBuffer.append(c);
                            }
                            wasWhiteSpace = false;
                            break;
            }
        }
        if (comment == MULTI_COMMENT) {
        	throw SmallSQLException.create(Language.STXADD_COMMENT_OPEN);
        }        
        if(!wasWhiteSpace) {
           tokens.add( new SQLToken( value, tokenStart, sql.length) );
        }
        
        return tokens;
    }

    static private void addKeyWord( String keyword, int value){
        keywords.put( Utils.getInteger( value), keyword );

        char[] letters = keyword.toCharArray();
        if(searchTree == null){
            searchTree = new SearchNode();
            searchTree.letter = (char)(letters[0] | 0x20);
        }
        SearchNode prev = null;
        SearchNode node = searchTree;
        boolean wasNextEntry = true;
        for(int i=0; i<letters.length; i++){
            char c = (char)(letters[i] | 0x20);
            while(node != null && node.letter != c) {
                prev = node;
                node = node.nextEntry;
                wasNextEntry = true;
            }
            if(node == null){
                node = new SearchNode();
                node.letter = c;
                if(wasNextEntry)
                     prev.nextEntry = node;
                else prev.nextLetter = node;
                wasNextEntry = false;
                prev = node;
                node = null;
            }else{
                prev = node;
                node = node.nextLetter;
                wasNextEntry = false;
            }
        }
        prev.value = value;
    }

	static final String getKeyWord(int key){
		return (String)keywords.get( Utils.getInteger(key) );
	}
	
	static final int getSQLDataType(int type){
		// on change of this map the order from getTypeInfo need to be change
		switch(type){
			case SQLTokenizer.BIT:
						return Types.BIT;
			case SQLTokenizer.BOOLEAN:
						return Types.BOOLEAN;
			case SQLTokenizer.BINARY:
						return Types.BINARY;
			case SQLTokenizer.VARBINARY:
						return Types.VARBINARY;
			case SQLTokenizer.LONGVARBINARY:
						return Types.LONGVARBINARY;
			case SQLTokenizer.BLOB:
						return Types.BLOB;
			case SQLTokenizer.TINYINT:
						return Types.TINYINT;
			case SQLTokenizer.SMALLINT:
						return Types.SMALLINT;
			case SQLTokenizer.INT:
						return Types.INTEGER;
			case SQLTokenizer.BIGINT:
						return Types.BIGINT;
			case SQLTokenizer.SMALLMONEY:
			case SQLTokenizer.MONEY:
			case SQLTokenizer.DECIMAL:
						return Types.DECIMAL;
			case SQLTokenizer.NUMERIC:
						return Types.NUMERIC;
			case SQLTokenizer.REAL:
						return Types.REAL;
			case SQLTokenizer.FLOAT:
						return Types.FLOAT;
			case SQLTokenizer.DOUBLE:
						return Types.DOUBLE;
			case SQLTokenizer.DATE:
						return Types.DATE;
			case SQLTokenizer.TIME:
						return Types.TIME;
			case SQLTokenizer.TIMESTAMP:
			case SQLTokenizer.SMALLDATETIME:
						return Types.TIMESTAMP;
			case SQLTokenizer.CHAR:
			case SQLTokenizer.NCHAR:
						return Types.CHAR;
			case SQLTokenizer.VARCHAR:
			case SQLTokenizer.NVARCHAR:
						return Types.VARCHAR;
			case SQLTokenizer.LONGNVARCHAR:
			case SQLTokenizer.LONGVARCHAR:
						return Types.LONGVARCHAR;
			case SQLTokenizer.CLOB:
			case SQLTokenizer.NCLOB:
						return Types.CLOB;
			case SQLTokenizer.JAVA_OBJECT:
						return Types.JAVA_OBJECT;
			case SQLTokenizer.UNIQUEIDENTIFIER:
						return -11;
			case SQLTokenizer.NULL:
						return Types.NULL;
			default: throw new Error("DataType:"+type);
		}
	}
	
    static SearchNode searchTree;

    static Hashtable keywords = new Hashtable(337);
    static final int VALUE      = 0;
    static final int STRING     = 3;
    static final int IDENTIFIER  = 4;
    static final int NUMBERVALUE= 5;
    static{
        // for Error messages
        keywords.put( new Integer(VALUE),       "<expression>" );
        keywords.put( new Integer(IDENTIFIER),   "<identifier>" );
        keywords.put( new Integer(NUMBERVALUE), "<number>" );
    }

    static final int PERCENT        = '%'; // 37
    static final int BIT_AND        = '&'; // 38
    static final int PARENTHESIS_L  = '('; // 40
    static final int PARENTHESIS_R  = ')'; // 41
    static final int ASTERISK       = '*'; // 42
    static final int PLUS           = '+'; // 43
    static final int COMMA          = ','; // 44
    static final int MINUS          = '-'; // 45
    static final int POINT          = '.'; // 46
    static final int SLACH          = '/'; // 47
    static final int LESSER         = '<'; // 60
    static final int EQUALS         = '='; // 61
    static final int GREATER        = '>'; // 62
    static final int QUESTION       = '?'; // 63
    static final int BIT_XOR        = '^'; // 94
    static final int ESCAPE_L       = '{'; // 123
    static final int BIT_OR         = '|'; // 124
    static final int ESCAPE_R       = '}'; // 125
    static final int TILDE          = '~'; // 126

    static final int LESSER_EQU     = 100 + LESSER; // <=
    static final int UNEQUALS       = 100 + EQUALS;  // <>
    static final int GREATER_EQU    = 100 + GREATER; // >=

	static{
		// for error messages
		keywords.put( new Integer(LESSER_EQU),	"<=" );
		keywords.put( new Integer(UNEQUALS),   	"<>" );
		keywords.put( new Integer(GREATER_EQU), ">=" );
	}
    static final int SELECT     = 200;
    static final int DELETE     = 201;
    //static final int INSERT     = 202;
    static final int INTO       = 203;
    static final int UPDATE     = 204;
    static final int CREATE     = 205;
    static final int DROP       = 206;
    static final int ALTER      = 207;
    static final int SET        = 208;
    static final int EXECUTE    = 209;
    static final int FROM       = 210;
    static final int WHERE      = 211;
    static final int GROUP      = 212;
    static final int BY         = 213;
    static final int HAVING     = 214;
    static final int ORDER      = 215;
	static final int ASC        = 216;
	static final int DESC       = 217;
    static final int VALUES     = 218;
    static final int AS         = 219;
    static final int DEFAULT    = 220;
    static final int IDENTITY   = 221;
    static final int INNER      = 222;
    static final int JOIN       = 223;
    static final int ON         = 224;
    static final int OUTER      = 225;
    static final int FULL       = 226;
    static final int CROSS      = 227;
    static final int USE		= 228;
	static final int TOP		= 229;
	static final int ADD		= 230;
	static final int LIMIT      = 231;

    static final int DATABASE   = 235;
    static final int TABLE      = 236;
    static final int VIEW       = 237;
    static final int INDEX      = 238;
    static final int PROCEDURE  = 239;

    static final int TRANSACTION= 240;
    static final int ISOLATION  = 241;
    static final int LEVEL      = 242;
    static final int READ       = 243;
    static final int COMMITTED  = 244;
    static final int UNCOMMITTED= 245;
    static final int REPEATABLE = 246;
    static final int SERIALIZABLE= 247;
    
    static final int CONSTRAINT = 250;
	static final int PRIMARY 	= 251;
	static final int FOREIGN 	= 252;
	static final int KEY 		= 253;
	static final int UNIQUE 	= 254;
	static final int CLUSTERED  = 255;
	static final int NONCLUSTERED=256;
	static final int REFERENCES = 257;

	static final int UNION 		= 260;
	static final int ALL 		= 261;
	static final int DISTINCT   = 262;
	static final int CASE 		= 263;
	static final int WHEN 		= 264;
	static final int THEN 		= 265;
	static final int ELSE 		= 266;
	static final int END 		= 267;
	static final int SWITCH 	= 268;
	
	static final String DESC_STR   = "DESC";
    static{
        addKeyWord( "SELECT",   SELECT);
        addKeyWord( "DELETE",   DELETE);
//        addKeyWord( "INSERT",   INSERT);
        addKeyWord( "INTO",     INTO);
        addKeyWord( "UPDATE",   UPDATE);
        addKeyWord( "CREATE",   CREATE);
        addKeyWord( "DROP",     DROP);
        addKeyWord( "ALTER",    ALTER);
        addKeyWord( "SET",      SET);
        addKeyWord( "EXEC",     EXECUTE); // alias for EXECUTE; alias must set first so that key is in the hashtable and not the alias
        addKeyWord( "EXECUTE",  EXECUTE);
        addKeyWord( "FROM",     FROM);
        addKeyWord( "WHERE",    WHERE);
        addKeyWord( "GROUP",    GROUP);
        addKeyWord( "BY",       BY);
        addKeyWord( "HAVING",   HAVING);
        addKeyWord( "ORDER",    ORDER);
		addKeyWord( "ASC",      ASC);
		addKeyWord( DESC_STR,   DESC);
        addKeyWord( "VALUES",   VALUES);
        addKeyWord( "AS",       AS);
        addKeyWord( "DEFAULT",  DEFAULT);
        addKeyWord( "AUTO_INCREMENT", IDENTITY); // alias for IDENTITY; alias must set first so that key is in the hashtable and not the alias
        addKeyWord( "IDENTITY", IDENTITY);
        addKeyWord( "INNER",    INNER);
        addKeyWord( "JOIN",     JOIN);
        addKeyWord( "ON",       ON);
        addKeyWord( "OUTER",    OUTER);
        addKeyWord( "FULL",     FULL);
        addKeyWord( "CROSS",    CROSS);
		addKeyWord( "USE",      USE);
		addKeyWord( "TOP",      TOP);
		addKeyWord( "ADD",      ADD);
        addKeyWord( "LIMIT",    LIMIT);

        addKeyWord( "DATABASE", DATABASE);
        addKeyWord( "TABLE",    TABLE);
        addKeyWord( "VIEW",     VIEW);
        addKeyWord( "INDEX",    INDEX);
        addKeyWord( "PROCEDURE",PROCEDURE);

        addKeyWord( "TRANSACTION",  TRANSACTION);
        addKeyWord( "ISOLATION",    ISOLATION);
        addKeyWord( "LEVEL",        LEVEL);
        addKeyWord( "READ",         READ);
        addKeyWord( "COMMITTED",    COMMITTED);
        addKeyWord( "UNCOMMITTED",  UNCOMMITTED);
        addKeyWord( "REPEATABLE",   REPEATABLE);
        addKeyWord( "SERIALIZABLE", SERIALIZABLE);
        
		addKeyWord( "CONSTRAINT", 	CONSTRAINT);
		addKeyWord( "PRIMARY",    	PRIMARY);
		addKeyWord( "FOREIGN",     	FOREIGN);
		addKeyWord( "KEY",    		KEY);
		addKeyWord( "UNIQUE",		UNIQUE);
		addKeyWord( "CLUSTERED",  	CLUSTERED);
		addKeyWord( "NONCLUSTERED", NONCLUSTERED);
		addKeyWord( "REFERENCES",   REFERENCES);

		addKeyWord( "UNION", 		UNION);
		addKeyWord( "ALL",   		ALL);
		addKeyWord( "DISTINCT",   	DISTINCT);
		addKeyWord( "CASE",   		CASE);
		addKeyWord( "WHEN",   		WHEN);
		addKeyWord( "THEN",   		THEN);
		addKeyWord( "ELSE",   		ELSE);
		addKeyWord( "END",   		END);
		addKeyWord( "SWITCH", 		SWITCH);
    }


    //data types
    static final int BIT            = 300;
    static final int BOOLEAN        = 301;
    static final int BINARY         = 310;
    static final int VARBINARY      = 311;
	static final int RAW      		= 312;
    static final int LONGVARBINARY  = 313;
    static final int BLOB           = 316;
    static final int TINYINT        = 321;
    static final int SMALLINT       = 322;
    static final int INT            = 323;
    static final int COUNTER        = 324; //alias for INT IDENTITY, is used from MS ACCESS
    static final int BIGINT         = 325;
    static final int SMALLMONEY     = 330;
    static final int MONEY          = 331;
    static final int DECIMAL        = 332;
    static final int NUMERIC        = 333;
    static final int REAL           = 336;
    static final int FLOAT          = 337;
    static final int DOUBLE         = 338;
    static final int DATE           = 340;
    static final int TIME           = 341;
    static final int TIMESTAMP      = 342;
    static final int SMALLDATETIME  = 343;
    static final int CHAR           = 350;
    static final int NCHAR          = 352;
    static final int VARCHAR        = 353;
    static final int NVARCHAR       = 355;
	static final int SYSNAME        = 357;
    static final int LONGVARCHAR    = 359;
    static final int LONGNVARCHAR   = 360;
    static final int LONG           = 361;
    static final int CLOB           = 362;
    static final int NCLOB          = 363;
    static final int UNIQUEIDENTIFIER= 370;
    static final int JAVA_OBJECT    = 371;

    static{
        addKeyWord( "BIT",          BIT);
        addKeyWord( "BOOLEAN",      BOOLEAN);
        addKeyWord( "BINARY",       BINARY);
        addKeyWord( "VARBINARY",    VARBINARY);
		addKeyWord( "RAW",          RAW); // alias for Oracle RAW and LONG RAW
		addKeyWord( "IMAGE",        LONGVARBINARY); // alias for MS SQL Server data type IMAGE
        addKeyWord( "LONGVARBINARY",LONGVARBINARY);
        addKeyWord( "BLOB",         BLOB);
        addKeyWord( "BYTE",         TINYINT);
        addKeyWord( "TINYINT",      TINYINT);
        addKeyWord( "SMALLINT",     SMALLINT);
		addKeyWord( "INTEGER",      INT);
        addKeyWord( "INT",          INT);
        addKeyWord( "SERIAL",       COUNTER); //alias for MySQL and PostgreSQL
        addKeyWord( "COUNTER",      COUNTER);
        addKeyWord( "BIGINT",       BIGINT);
        addKeyWord( "SMALLMONEY",   SMALLMONEY);
        addKeyWord( "MONEY",        MONEY);
		addKeyWord( "NUMBER",       DECIMAL);
		addKeyWord( "VARNUM",       DECIMAL);
		addKeyWord( "DECIMAL",      DECIMAL);
        addKeyWord( "NUMERIC",      NUMERIC);
        addKeyWord( "REAL",         REAL);
        addKeyWord( "FLOAT",        FLOAT);
        addKeyWord( "DOUBLE",       DOUBLE);
        addKeyWord( "DATE",         DATE);
        addKeyWord( "TIME",         TIME);
        addKeyWord( "DATETIME",     TIMESTAMP); // alias for MS SQL Server data type DATETIME
        addKeyWord( "TIMESTAMP",    TIMESTAMP);
        addKeyWord( "SMALLDATETIME",SMALLDATETIME);
        addKeyWord( "CHARACTER",    CHAR); // alias for CHAR
        addKeyWord( "CHAR",         CHAR);
        addKeyWord( "NCHAR",        NCHAR);
        addKeyWord( "VARCHAR2",     VARCHAR); // alias for Oracle VARCHAR2
        addKeyWord( "VARCHAR",      VARCHAR);
        addKeyWord( "NVARCHAR2",    NVARCHAR); // alias for Oracle VARCHAR2
        addKeyWord( "NVARCHAR",     NVARCHAR);
		addKeyWord( "SYSNAME",      SYSNAME);
        addKeyWord( "TEXT",         LONGVARCHAR);
        addKeyWord( "LONGVARCHAR",  LONGVARCHAR);
        addKeyWord( "NTEXT",        LONGNVARCHAR);
        addKeyWord( "LONGNVARCHAR", LONGNVARCHAR);
        addKeyWord( "LONG",         LONG); // alias for Oracle LONG and LONG RAW
        addKeyWord( "CLOB",         CLOB);
        addKeyWord( "NCLOB",        NCLOB);
        addKeyWord( "UNIQUEIDENTIFIER",UNIQUEIDENTIFIER);
        addKeyWord( "SQL_VARIANT",  JAVA_OBJECT); // alias for MS SQL Server data type SQL_VARIANT
        addKeyWord( "JAVA_OBJECT",  JAVA_OBJECT);
    }

    //escape commands
    static final int D      = 400;
    static final int T      = 401;
    static final int TS     = 402;
    static final int FN     = 403;
    static final int CALL   = 404;
    static final int OJ     = 405;
    static{
        addKeyWord( "D",    D);
        addKeyWord( "T",    T);
        addKeyWord( "TS",   TS);
        addKeyWord( "FN",   FN);
        addKeyWord( "CALL", CALL);
        addKeyWord( "OJ", OJ);
    }


    static final int OR     = 500;
    static final int AND    = 501;
	static final int IS     = 502;
    static final int NOT    = 503;
    static final int NULL   = 504;
    static final int TRUE   = 505;
    static final int FALSE  = 506;
    static final int BETWEEN= 507;
    static final int LIKE   = 508;
    static final int IN     = 509;
    static{
        addKeyWord( "OR",       OR);
        addKeyWord( "AND",      AND);
		addKeyWord( "IS",       IS);
        addKeyWord( "NOT",      NOT);
        addKeyWord( "NULL",     NULL);
		addKeyWord( "YES",      TRUE); //alias for TRUE
        addKeyWord( "TRUE",     TRUE);
		addKeyWord( "NO",    	FALSE); //alias for FALSE
        addKeyWord( "FALSE",    FALSE);
        addKeyWord( "BETWEEN",  BETWEEN);
        addKeyWord( "LIKE",     LIKE);
        addKeyWord( "IN",       IN);
    }


    //NUMERIC FUNCTIONS
    static final int ABS        = 1000; //first numeric function --> see SSDatabaseMetaData.getNumericFunctions
    static final int ACOS       = 1001;
    static final int ASIN       = 1002;
    static final int ATAN       = 1003;
    static final int ATAN2      = 1004;
    static final int CEILING    = 1005;
    static final int COS        = 1006;
    static final int COT        = 1007;
    static final int DEGREES    = 1008;
    static final int EXP        = 1009;
    static final int FLOOR      = 1010;
    static final int LOG        = 1011;
    static final int LOG10      = 1012;
    static final int MOD        = 1013;
    static final int PI         = 1014;
    static final int POWER      = 1015;
    static final int RADIANS    = 1016;
    static final int RAND       = 1017;
    static final int ROUND      = 1018;
    static final int SIGN       = 1019;
    static final int SIN        = 1020;
    static final int SQRT       = 1021;
    static final int TAN        = 1022;
    static final int TRUNCATE   = 1023; //last numeric function --> see SSDatabaseMetaData.getNumericFunctions
    static{
        addKeyWord( "ABS",      ABS);
        addKeyWord( "ACOS",     ACOS);
        addKeyWord( "ASIN",     ASIN);
        addKeyWord( "ATAN",     ATAN);
        addKeyWord( "ATN2",    	ATAN2); //alias for MS SQL Server
        addKeyWord( "ATAN2",    ATAN2);
        addKeyWord( "CEILING",  CEILING);
        addKeyWord( "COS",      COS);
        addKeyWord( "COT",      COT);
        addKeyWord( "DEGREES",  DEGREES);
        addKeyWord( "EXP",      EXP);
        addKeyWord( "FLOOR",    FLOOR);
        addKeyWord( "LOG",      LOG);
        addKeyWord( "LOG10",    LOG10);
        addKeyWord( "MOD",      MOD);
        addKeyWord( "PI",       PI);
        addKeyWord( "POWER",    POWER);
        addKeyWord( "RADIANS",  RADIANS);
        addKeyWord( "RAND",     RAND);
        addKeyWord( "ROUND",    ROUND);
        addKeyWord( "SIGN",     SIGN);
        addKeyWord( "SIN",      SIN);
        addKeyWord( "SQRT",     SQRT);
        addKeyWord( "TAN",      TAN);
        addKeyWord( "TRUNCATE", TRUNCATE);
    }

    //String Functions
    static final int ASCII      = 1100; //first string function --> see SSDatabaseMetaData.getStringFunctions
    static final int BITLEN     = 1101;
    static final int CHARLEN    = 1102;
    static final int CHARACTLEN = 1103;
    static final int _CHAR      = 1104;
    static final int CONCAT     = 1105;
    static final int DIFFERENCE = 1106;
    static final int INSERT     = 1107;
    static final int LCASE      = 1108;
    static final int LEFT       = 1109;
    static final int LENGTH     = 1110;
    static final int LOCATE     = 1111;
    static final int LTRIM      = 1112;
    static final int OCTETLEN   = 1113;
    static final int REPEAT     = 1114;
    static final int REPLACE    = 1115;
    static final int RIGHT      = 1116;
    static final int RTRIM      = 1117;
    static final int SOUNDEX    = 1118;
    static final int SPACE      = 1119;
    static final int SUBSTRING  = 1120;
    static final int TRIM       = 1121;
    static final int UCASE      = 1122; //last string function --> see SSDatabaseMetaData.getStringFunctions
    static{
        addKeyWord( "ASCII",    ASCII);
        addKeyWord( "BIT_LENGTH", BITLEN);
        addKeyWord( "CHAR_LENGTH", CHARLEN);
        addKeyWord( "CHARACTER_LENGTH", CHARACTLEN);
		keywords.put( new Integer(_CHAR), "CHAR" ); // needed for meta data functions
        addKeyWord( "CONCAT",   CONCAT);
        addKeyWord( "DIFFERENCE",DIFFERENCE);
		addKeyWord( "STUFF",    INSERT); //alias for MS SQL Server
		addKeyWord( "INSERT",   INSERT);
        addKeyWord( "LCASE",    LCASE);
		addKeyWord( "LEFT",     LEFT);
		addKeyWord( "DATALENGTH",LENGTH); //alias for MS SQL Server
		addKeyWord( "LEN",		LENGTH); //alias for MS SQL Server
        addKeyWord( "LENGTH",   LENGTH);
		addKeyWord( "CHARINDEX",LOCATE); //alias for MS SQL Server
        addKeyWord( "LOCATE",   LOCATE);
        addKeyWord( "LTRIM",    LTRIM);
        addKeyWord( "OCTET_LENGTH", OCTETLEN);
        addKeyWord( "REPEAT",   REPEAT);
        addKeyWord( "REPLACE",  REPLACE);
		addKeyWord( "RIGHT",    RIGHT);
        addKeyWord( "RTRIM",    RTRIM);
        addKeyWord( "SOUNDEX",  SOUNDEX);
        addKeyWord( "SPACE",    SPACE);
        addKeyWord( "SUBSTRING",SUBSTRING);
        addKeyWord( "TRIM",     TRIM);
        addKeyWord( "UCASE",    UCASE);
    }

    //TIME and DATE FUNCTIONS
    static final int CURDATE    = 1200; //first time date function --> see SSDatabaseMetaData.getTimeDateFunctions
    static final int CURRENTDATE = 1201;
    static final int CURTIME    = 1202;
    static final int DAYNAME    = 1203;
    static final int DAYOFMONTH = 1204;
    static final int DAYOFWEEK  = 1205;
    static final int DAYOFYEAR  = 1206;
	static final int DAY		= 1207;
    static final int HOUR       = 1208;
	static final int MILLISECOND= 1209;
    static final int MINUTE     = 1210;
    static final int MONTH      = 1211;
    static final int MONTHNAME  = 1212;
    static final int NOW        = 1213;
    static final int QUARTER    = 1214;
    static final int SECOND     = 1215;
    static final int TIMESTAMPADD=1216;
    static final int TIMESTAMPDIFF=1217;
    static final int WEEK       = 1218;
    static final int YEAR       = 1219; //last time date function --> see SSDatabaseMetaData.getTimeDateFunctions
    static{
        addKeyWord( "CURDATE",      CURDATE);
        addKeyWord( "CURTIME",      CURTIME);
        addKeyWord( "CURRENT_DATE", CURRENTDATE);
        addKeyWord( "DAYNAME",      DAYNAME);
        addKeyWord( "DAYOFMONTH",   DAYOFMONTH);
        addKeyWord( "DAYOFWEEK",    DAYOFWEEK);
        addKeyWord( "DAYOFYEAR",    DAYOFYEAR);
		addKeyWord( "DAY",    		DAY);
        addKeyWord( "HOUR",         HOUR);
		addKeyWord( "MILLISECOND",  MILLISECOND);
        addKeyWord( "MINUTE",       MINUTE);
        addKeyWord( "MONTH",        MONTH);
        addKeyWord( "MONTHNAME",    MONTHNAME);
		addKeyWord( "GETDATE",      NOW); //alias for MS SQL Server
        addKeyWord( "NOW",          NOW);
        addKeyWord( "QUARTER",      QUARTER);
        addKeyWord( "SECOND",       SECOND);
		addKeyWord( "DATEADD", 		TIMESTAMPADD); //alias for MS SQL Server
        addKeyWord( "TIMESTAMPADD", TIMESTAMPADD);
		addKeyWord( "DATEDIFF",		TIMESTAMPDIFF); //alias for MS SQL Server
        addKeyWord( "TIMESTAMPDIFF",TIMESTAMPDIFF);
        addKeyWord( "WEEK",         WEEK);
        addKeyWord( "YEAR",         YEAR);
    }
    
    // Time intervals
	static final int SQL_TSI_FRAC_SECOND= 1250;
	static final int SQL_TSI_SECOND		= 1251;
	static final int SQL_TSI_MINUTE		= 1252;
	static final int SQL_TSI_HOUR		= 1253;
	static final int SQL_TSI_DAY		= 1254;
	static final int SQL_TSI_WEEK		= 1255;
	static final int SQL_TSI_MONTH		= 1256;
	static final int SQL_TSI_QUARTER	= 1257;
	static final int SQL_TSI_YEAR		= 1258;
	static{
		addKeyWord( "MS",					SQL_TSI_FRAC_SECOND);
		addKeyWord( "SQL_TSI_FRAC_SECOND",	SQL_TSI_FRAC_SECOND);
		addKeyWord( "S",					SQL_TSI_SECOND);
		addKeyWord( "SS",					SQL_TSI_SECOND);
		addKeyWord( "SQL_TSI_SECOND",		SQL_TSI_SECOND);
		addKeyWord( "MI",					SQL_TSI_MINUTE);
		addKeyWord( "N",					SQL_TSI_MINUTE);
		addKeyWord( "SQL_TSI_MINUTE",		SQL_TSI_MINUTE);
		addKeyWord( "HH",					SQL_TSI_HOUR);
		addKeyWord( "SQL_TSI_HOUR",			SQL_TSI_HOUR);
		//addKeyWord( "D",					SQL_TSI_DAY);
		addKeyWord( "DD",					SQL_TSI_DAY);
		addKeyWord( "SQL_TSI_DAY",			SQL_TSI_DAY);
		addKeyWord( "WK",					SQL_TSI_WEEK);
		addKeyWord( "WW",					SQL_TSI_WEEK);
		addKeyWord( "SQL_TSI_WEEK",			SQL_TSI_WEEK);
		addKeyWord( "M",					SQL_TSI_MONTH);
		addKeyWord( "MM",					SQL_TSI_MONTH);
		addKeyWord( "SQL_TSI_MONTH",		SQL_TSI_MONTH);
		addKeyWord( "Q",					SQL_TSI_QUARTER);
		addKeyWord( "QQ",					SQL_TSI_QUARTER);
		addKeyWord( "SQL_TSI_QUARTER",		SQL_TSI_QUARTER);
		addKeyWord( "YY",					SQL_TSI_YEAR);
		addKeyWord( "YYYY",					SQL_TSI_YEAR);
		addKeyWord( "SQL_TSI_YEAR",			SQL_TSI_YEAR);
	}
	
    //SYSTEM FUNCTIONS
    //static final int DATABASE   = 1300;
    static final int IFNULL     = 1301; //first system function --> see SSDatabaseMetaData.getSystemFunctions
    static final int USER       = 1302;
    static final int CONVERT    = 1303;
    static final int CAST    	= 1304;
	static final int IIF    	= 1305; //last system function --> see SSDatabaseMetaData.getSystemFunctions
    static{
		addKeyWord( "ISNULL",      	IFNULL); //alias for IFNULL, used from MS SQL Server with 2 parameter, from MS Access with 1 parameter
        addKeyWord( "IFNULL",       IFNULL);
        addKeyWord( "USER",         USER);
        addKeyWord( "CONVERT",      CONVERT);
        addKeyWord( "CAST",      	CAST);
		addKeyWord( "IIF",      	IIF);
    }
    
    // data types for escape function CONVERT
	static final int SQL_BIGINT    		= 1350;
	static final int SQL_BINARY    		= 1351;
	static final int SQL_BIT    		= 1352;
	static final int SQL_CHAR    		= 1353;
	static final int SQL_DATE    		= 1354;
	static final int SQL_DECIMAL    	= 1355;
	static final int SQL_DOUBLE    		= 1356;
	static final int SQL_FLOAT    		= 1357;
	static final int SQL_INTEGER    	= 1358;
	static final int SQL_LONGVARBINARY 	= 1359;
	static final int SQL_LONGVARCHAR 	= 1360;
	static final int SQL_REAL    		= 1361;
	static final int SQL_SMALLINT    	= 1362;
	static final int SQL_TIME    		= 1363;
	static final int SQL_TIMESTAMP    	= 1364;
	static final int SQL_TINYINT    	= 1365;
	static final int SQL_VARBINARY    	= 1366;
	static final int SQL_VARCHAR    	= 1367;
	static{
		addKeyWord( "SQL_BIGINT",		SQL_BIGINT);
		addKeyWord( "SQL_BINARY",		SQL_BINARY);
		addKeyWord( "SQL_BIT",			SQL_BIT);
		addKeyWord( "SQL_CHAR",			SQL_CHAR);
		addKeyWord( "SQL_DATE",			SQL_DATE);
		addKeyWord( "SQL_DECIMAL",		SQL_DECIMAL);
		addKeyWord( "SQL_DOUBLE",		SQL_DOUBLE);
		addKeyWord( "SQL_FLOAT",		SQL_FLOAT);
		addKeyWord( "SQL_INTEGER",		SQL_INTEGER);
		addKeyWord( "SQL_LONGVARBINARY",SQL_LONGVARBINARY);
		addKeyWord( "SQL_LONGVARCHAR",	SQL_LONGVARCHAR);
		addKeyWord( "SQL_REAL",			SQL_REAL);
		addKeyWord( "SQL_SMALLINT",		SQL_SMALLINT);
		addKeyWord( "SQL_TIME",			SQL_TIME);
		addKeyWord( "SQL_TIMESTAMP",	SQL_TIMESTAMP);
		addKeyWord( "SQL_TINYINT",		SQL_TINYINT);
		addKeyWord( "SQL_VARBINARY",	SQL_VARBINARY);
		addKeyWord( "SQL_VARCHAR",		SQL_VARCHAR);
	}
	
    
    //Aggregate Function
    static final int COUNT		= 1400;
	static final int MIN		= 1401;
	static final int MAX		= 1402;
    static final int SUM		= 1403;
	static final int FIRST		= 1404;
	static final int LAST		= 1405;
	static final int AVG		= 1406;
	static{
		addKeyWord( "COUNT",       	COUNT);
		addKeyWord( "MIN",      	MIN);
		addKeyWord( "MAX",      	MAX);
		addKeyWord( "SUM",         	SUM);
		addKeyWord( "FIRST",        FIRST);
		addKeyWord( "LAST",         LAST);
		addKeyWord( "AVG",          AVG);
	}

}

class SearchNode{
    int value;
    char letter;
    SearchNode nextLetter; // next character of a keyword
    SearchNode nextEntry;  // next Entry of a character that has the same start sequence
}


