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
 * TestLanguage.java
 * ---------------
 * Author: Saverio Miroddi
 * 
 */
package smallsql.junit;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Set;

import smallsql.database.language.Language;

/**
 * Test for language resources.<br>
 * <b>Messages to console starting with "REGULAR" are just informational messages.</b>
 * 
 * @author Saverio Miroddi
 */
public class TestLanguage extends BasicTestCase {
	private static final String TABLE_NAME = "test_lang";
	
	private static final String[] OTHER_LANGUAGES = { "it", "de" };
	
	public void setUp() throws SQLException {
		tearDown();
	}
	
	public void tearDown() throws SQLException {
		// restore language
		Connection conn = AllTests.createConnection("?locale=en", null);
		
		try {
			conn.prepareStatement("DROP TABLE " + TABLE_NAME).execute();
		}
		catch (Exception e) {}
		finally {
			conn.close();
		}
	}
	
	/**
	 * In case of wrong Locale, Language picks up the one corresponding to the
	 * the current Locale.<br>
	 */
	public void testBogusLocale() throws SQLException {
		Locale origLocale = Locale.getDefault();
		Locale.setDefault(Locale.ITALY);
		
		Connection conn = AllTests.createConnection("?locale=XXX", null);
		Statement stat = conn.createStatement();

		try {
			recreateTestTab(stat);
			
			stat.execute("CREATE TABLE " + TABLE_NAME + " (id_test INT)");
			fail();
		}
		catch (SQLException e) {
			assertMessage(e, "La tabella/vista '" + TABLE_NAME + "' è già esistente.");
		}
		finally {
			Locale.setDefault(origLocale);
			conn.close();
		}
	}
	
	public void testLocalizedErrors() throws Exception {
		Connection conn = AllTests.createConnection("?locale=it", null);
		Statement stat = conn.createStatement();
		
		try {
			try {
				recreateTestTab(stat);
	
				stat.execute("CREATE TABLE " + TABLE_NAME + " (id_test INT)");
				fail();
			}
			catch(SQLException e) {
				assertMessage(e, "La tabella/vista '" + TABLE_NAME + "' è già esistente.");
			}
			
			try {
				stat.execute("DROP TABLE " + TABLE_NAME);
				stat.execute("DROP TABLE " + TABLE_NAME);
			}
			catch (SQLException e) {
				assertMessage(e, "Non si può effettuare DROP della tabella");
			}
			
			try {
				stat.execute("CREATE TABLE foo");
			}
			catch (SQLException e) {
				assertMessage(e, "Errore di sintassi, fine inattesa");
			}
		}
		finally {
			conn.close();
		}
	}
	
	public void testSyntaxErrors() throws SQLException {
		Connection conn = AllTests.createConnection("?locale=it", null);
		Statement stat = conn.createStatement();
		
		try {
			try {
				stat.execute("CREATE TABLE");
			}
			catch (SQLException se) {
				assertMessage(se, "Errore di sintassi, fine inattesa della stringa SQL. Le parole chiave richieste sono: <identifier>");
			}
			
			try {
				stat.execute("Some nonsensical sentence.");
			}
			catch (SQLException se) {
				assertMessage(se, "Errore di sintassi alla posizione 0 in 'Some'. Le parole chiave richieste sono");
			}

			recreateTestTab(stat);
			
			try {
				stat.execute("SELECT bar() FROM foo");
			}
			catch (SQLException se) {
				assertMessage(se, "Errore di sintassi alla posizione 7 in 'bar'. Funzione sconosciuta");
			}
			
			try {
				stat.execute("SELECT UCASE('a', '');");
			}
			catch (SQLException se) {
				assertMessage(se, "Errore di sintassi alla posizione 7 in 'UCASE'. Totale parametri non valido.");
			}
		}
		finally {
			conn.close();
		}
	}
	
	/**
	 * Check if the passed text is present inside the exception message and
	 * prints the message in System.out
	 */
	private void assertMessage(SQLException e, String expectedText) {
		assertMessage(e, new String[] { expectedText });
	}
	
	/**
	 * Check if the passed texts are present inside the exception message and
	 * prints the message in System.out
	 */
	private void assertMessage(SQLException e, String[] expectedTexts) {
		String message = e.getMessage();
		boolean found = true;
		
		for (int i = 0; found && i < expectedTexts.length; i++) {
			found = found && message.indexOf(expectedTexts[i]) >= 0;
		}
		
		if (! found) {
			System.err.println("ERROR [Wrong message]:" + message);
			fail();
		}
	}
	
	private void recreateTestTab(Statement stat) throws SQLException {
		stat.execute("CREATE TABLE " + TABLE_NAME + " (id_test INT)");

	}
	
    
	/**
	 * Checks if languages specified by OTHER_LANGUAGES array translate all the
	 * message entries of the base (English) language: if they don't, the test
	 * fails and print the missing ones in System.err.
	 */
	public void testEntries() throws Exception {
		boolean failed = false;
        StringBuffer msgBuf = new StringBuffer();
		Language eng = Language.getLanguage("en"); 
        
        HashSet engEntriesSet = new HashSet();
        String[][] engEntriesArr = eng.getEntries();
        /* WARNING! skips message 0 (CUSTOM_MESSAGE) */
        for (int j = 1; j < engEntriesArr.length; j++) {
            engEntriesSet.add(engEntriesArr[j][0]);
        }
		
		for (int i = 0; i < OTHER_LANGUAGES.length; i++) {
			String localeStr = OTHER_LANGUAGES[i];
			Language lang2 = Language.getLanguage(localeStr);
            
            HashSet otherEntriesSet = new HashSet();        
            String[][] otherEntriesArr = lang2.getEntries();        
            for (int j = 0; j < otherEntriesArr.length; j++) {
                otherEntriesSet.add(otherEntriesArr[j][0]);
            }
            
            /* test missing entries */
			Set diff = (Set)engEntriesSet.clone();
            diff.removeAll(otherEntriesSet);

			if (diff.size() > 0) {
				failed = true;
                msgBuf.append("\nMissing entries for language ").append( OTHER_LANGUAGES[i] ).append(": ");
				
				for (Iterator itr = diff.iterator(); itr.hasNext(); ) {
					msgBuf.append(itr.next());
					if (itr.hasNext()) msgBuf.append(',');
				}
			}
            
            /* test additional entries */
            diff = (Set)otherEntriesSet.clone();
            diff.removeAll(engEntriesSet);

            if (diff.size() > 0) {
                failed = true;
                msgBuf.append("\nAdditional entries for language ").append( OTHER_LANGUAGES[i] ).append(": ");
                
                for (Iterator itr = diff.iterator(); itr.hasNext(); ) {
                    msgBuf.append(itr.next());
                    if (itr.hasNext()) msgBuf.append(',');
                }
            }
            
            /* test not translated entries */
            StringBuffer buf = new StringBuffer();
            for (int j = 1; j < engEntriesArr.length; j++) {
                String key = engEntriesArr[j][0];
                String engValue = eng.getMessage(key);
                String otherValue = lang2.getMessage(key);
                if(engValue.equals(otherValue)){
                    failed = true;
                    if(buf.length() > 0){
                        buf.append(',');
                    }
                    buf.append(key);
                }
            }
            if(buf.length()>0){
                msgBuf.append("\nNot translated entries for language ").append( OTHER_LANGUAGES[i] ).append(": ");
                msgBuf.append(buf);
            }
		}		

		if (failed){
            System.err.println(msgBuf);
            fail(msgBuf.toString());
        }
	}
}
