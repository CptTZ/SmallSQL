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
 * AllTests.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.junit;

import junit.framework.*;
import java.sql.*;
import java.util.Properties;

public class AllTests extends TestCase{

    final static String CATALOG = "AllTests";
    final static String JDBC_URL = "jdbc:smallsql:" + CATALOG;
    private static Connection con;
    
    
    public static Connection getConnection() throws SQLException{
        if(con == null || con.isClosed()){
            con = createConnection();
        }
        return con;
    }
    
    /**
     * Creates a connection in the English locale.<br>
     */
	public static Connection createConnection() throws SQLException{
		//DriverManager.setLogStream( System.out );
		new smallsql.database.SSDriver();
		new sun.jdbc.odbc.JdbcOdbcDriver();
		return DriverManager.getConnection(JDBC_URL + "?create=true;locale=en");
		//return DriverManager.getConnection("jdbc:odbc:mssql","sa","");
	}

	/**
	 * Creates a connection, with the possibility of appending an additional
	 * string to the url and/or passing a Properties object.<br>
	 * Locale is not specified.
	 * 
	 * @param urlAddition
	 *            String to append to url; nullable.
	 * @param info
	 *            object Properties; nullable.
	 * @return connection created.
	 */
    public static Connection createConnection(String urlAddition, 
    		Properties info) 
    throws SQLException {
		new smallsql.database.SSDriver();
		new sun.jdbc.odbc.JdbcOdbcDriver();
		
		if (urlAddition == null) urlAddition = "";
		if (info == null) info = new Properties();
		
		String urlComplete = JDBC_URL + urlAddition;
		
		return DriverManager.getConnection(urlComplete, info);
    }
    
    public static void printRS( ResultSet rs ) throws SQLException{
        while(rs.next()){
            for(int i=1; i<=rs.getMetaData().getColumnCount(); i++){
                System.out.print(rs.getObject(i)+"\t");
            }
            System.out.println();
        }
    }

    public static Test suite() throws Exception{
        TestSuite theSuite = new TestSuite("SmallSQL all Tests");
        theSuite.addTestSuite( TestAlterTable.class );
        theSuite.addTestSuite( TestAlterTable2.class );
        theSuite.addTest    ( TestDataTypes.suite() );
        theSuite.addTestSuite(TestDBMetaData.class);
		theSuite.addTestSuite(TestExceptionMethods.class);
		theSuite.addTest     (TestExceptions.suite());
		theSuite.addTestSuite(TestDeleteUpdate.class);
		theSuite.addTest     (TestFunctions.suite() );
		theSuite.addTestSuite(TestGroupBy.class);
		theSuite.addTestSuite(TestIdentifer.class);
		theSuite.addTest     (TestJoins.suite());
        theSuite.addTestSuite(TestLanguage.class);
		theSuite.addTestSuite(TestMoneyRounding.class );
		theSuite.addTest     (TestOperatoren.suite() );
		theSuite.addTestSuite(TestOrderBy.class);
		theSuite.addTestSuite(TestOther.class);
        theSuite.addTestSuite(TestResultSet.class);
		theSuite.addTestSuite(TestScrollable.class);
        theSuite.addTestSuite(TestStatement.class);
        theSuite.addTestSuite(TestThreads.class);
        theSuite.addTestSuite(TestTokenizer.class);
        theSuite.addTestSuite(TestTransactions.class);
        return theSuite;
    }

    public static void main(String[] argv) {
    	try{
    		//junit.swingui.TestRunner.main(new String[]{AllTests.class.getName()});
    		junit.textui.TestRunner.main(new String[]{AllTests.class.getName()});
    	}catch(Throwable e){
    		e.printStackTrace();
    	}
    }

}