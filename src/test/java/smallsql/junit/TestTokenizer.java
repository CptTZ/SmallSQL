/* =============================================================
 * SmallSQL : a free Java DBMS library for the Java(tm) platform
 * =============================================================
 *
 * (C) Copyright 2004-2006, by Volker Berlin.
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
 * TestLanguage.java
 * ---------------
 * Author: Saverio Miroddi
 * 
 */
package smallsql.junit;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;

/**
 * Test for comments in statements.<br>
 * 
 * @author Saverio Miroddi
 */
public class TestTokenizer extends BasicTestCase {
	private static final String TABLE_NAME = "table_comments";
	private static final PrintStream out = System.out;
	
	private boolean init;
	private Connection conn;
	private Statement stat;
	
	public void setUp() throws SQLException {
		if (! init) {
			conn = AllTests.createConnection("?locale=en", null);
			stat = conn.createStatement();
			init = true;
		}
		dropTable();
		createTable();
	}
	
	public void tearDown() throws SQLException {
		if (conn != null) {
			dropTable();
			stat.close(); 
			conn.close();
		}
	}
	
	private void createTable() throws SQLException {
		stat.execute(
				"CREATE TABLE " + TABLE_NAME + 
				" (id INT, myint INT)");
		
		stat.execute(
				"INSERT INTO " + TABLE_NAME + " VALUES (1, 2)");
		stat.execute(
				"INSERT INTO " + TABLE_NAME + " VALUES (1, 3)");
	}
	
	private void dropTable() throws SQLException {
		try {
			stat.execute("DROP TABLE " + TABLE_NAME);
		} catch (SQLException e) {
			// just to check the error, if it happens, is the expected one
			out.println("REGULAR: " + e.getMessage() + '\n');
		}
	}
	
	public void testSingleLine() throws SQLException {
		final String SQL_1 = 
			"SELECT 10/2--mycomment\n" + 
			" , -- mycomment    \r\n" +
			"id, SUM(myint)--my comment  \n\n" +
			"FROM " + TABLE_NAME + " -- my other comment \r \r" + 
			"GROUP BY id --mycommentC\n" +
			"--   myC    omment  E    \n" +
			"ORDER BY id \r" +
			"--myCommentD   \r\r\r";
		
		successTest(SQL_1);

		final String SQL_2 = 
			"SELECT 10/2 - - this must fail ";
		
		failureTest(SQL_2, "Tokenized not-comment as a line-comment.");
	}
	
	public void testMultiLine() throws SQLException {
		final String SQL_1 = 
			"SELECT 10/2, id, SUM(myint) /* comment, 'ignore it.   \n" +
			" */ FROM /* -- comment */" + TABLE_NAME + " -- my comment /* \n\r" +
			" /* comment */ GROUP BY id ORDER BY id\r" +
			"/* comment */ -- somment\r\n";

		successTest(SQL_1);
		
		final String SQL_2 = 
			"SELECT 10/2 / * this must fail */";
		
		failureTest(SQL_2, "Tokenized not-comment as a multiline-comment.");

		final String SQL_3 = 
			"SELECT 10/2 /* this must fail ";
		
		failureTest(SQL_3, 
				"Uncomplete end multiline comment not recognized.",
				"Missing end comment mark");
	}
	
	private void successTest(String sql) throws SQLException {
//		out.println(SQL_1);
		
		ResultSet rs_1 = stat.executeQuery(sql);
		rs_1.next();
		rs_1.close();
	}
	
	private void failureTest(String sql, String failureMessage) {
		try {
			stat.executeQuery(sql);
			fail(failureMessage);
		}
		catch (SQLException e) {
			// just to check the error, if it happens, is the expected one
			out.println("REGULAR: " + e.getMessage() + '\n');
		}
	}
	
	private void failureTest(String sql, String failureMessage, String expected) {
		try {
			stat.executeQuery(sql);
			fail(failureMessage);
		}
		catch (SQLException e) {
			String foundMsg = e.getMessage();
			String assertMsg = MessageFormat.format(
					"Unexpected error: [{0}], expected: [{1}]", 
					new Object[] { foundMsg, expected }); 
			
			assertTrue(assertMsg, foundMsg.indexOf(expected) > -1);
			
			// just to check the error, if it happens, is the expected one
			out.println("REGULAR: " + e.getMessage() + '\n');
		}
	}
	//////////////////////////////////////////////////////////////////////
	// THE FOLLOWING TESTS NEED PACKAGE ACCESS, SO THEY ARE DISABLED. 
	//////////////////////////////////////////////////////////////////////
	
//	public void testCommentsInside() throws SQLException {
//		List parsedTokens;
//		char[] sourceSQL;
//		String[] expectedTokens;
//		int[] expTokensPos;
//		
//		/* test 1 */
//		
//		sourceSQL = ( 
//				"SELECT 10/2, id, SUM(myint) /* comment  \n" +
//				" */ FROM /* -- comment */" + TABLE_NAME + " -- my comment /* \n\r" +
//				" /* comment */ GROUP BY id ORDER BY id\r" +
//				"/* comment */ -- somment\r\n"
//			).toCharArray();
//	
//		expectedTokens = new String[]{ 
//			"SELECT", "10", "/", "2", ",", "id", ",", "SUM", "(", "myint", ")",
//			"FROM", TABLE_NAME, "GROUP", "BY", "id", "ORDER", "BY", "id",
//			};
//		expTokensPos = getPositions(sourceSQL, expectedTokens);
//		
//		parsedTokens = SQLTokenizer.parseSQL(sourceSQL);
//		printTokens(parsedTokens, sourceSQL);
//		check(parsedTokens, sourceSQL, expectedTokens, expTokensPos);
//	}
//	
//	/**
//	 * Very silly routine for avoding manual positions computation.<br>
//	 */
//	private int[] getPositions(char[] sourceSQLArr, String[] expectedTokens) {
//		String sourceSQL = String.valueOf(sourceSQLArr);
//		int[] expTokensPos = new int[expectedTokens.length]; 
//		
//		for (int i = 0, startPos = 0; i < expectedTokens.length; i++) {
//			String token = expectedTokens[i];
//			expTokensPos[i] = sourceSQL.indexOf(token, startPos);
//			if (expTokensPos[i] == -1) fail("token not found: " + token);
//			startPos = expTokensPos[i];
//		}
//		
//		return expTokensPos;
//	}
//	
//	private void check(List parsedTokens, char[] sourceSQL,	String[] expectedTokens, int[] expTokensPos) {
//		assertEquals("tokens number mismatch!", parsedTokens.size(), expectedTokens.length);
//		
//		int i = 0;
//		Iterator itr = parsedTokens.iterator();
//		while (itr.hasNext()) {
//			SQLToken parsedToken = (SQLToken)itr.next();
//			assertEquals(parsedToken.getName(sourceSQL), expectedTokens[i]);
//			assertEquals("Offset mismatch for " + expectedTokens[i], parsedToken.offset, expTokensPos[i]);
//			i++;
//		}
//	}
//	
//	private void printTokens(List parsedTokens, char[] sourceSQL) {
//		for (Iterator itr = parsedTokens.iterator(); itr.hasNext(); ) {
//			SQLToken parsedToken = (SQLToken)itr.next();
//			out.println('\'' + parsedToken.getName(sourceSQL) + "' (" + parsedToken.offset + ')');
//		}
//	}
}
