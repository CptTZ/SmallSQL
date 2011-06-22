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
 * TestIdentifer.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.junit;

import java.sql.*;

/**
 * @author Volker Berlin
 *
 */
public class TestIdentifer extends BasicTestCase {

	public TestIdentifer(){
		super();
	}
    
    
	public TestIdentifer(String arg0) {
		super(arg0);
	}
    

	public void testQuoteIdentifer() throws Exception{
		Connection con = AllTests.getConnection();
		dropTable(con,"QuoteIdentifer");
		con.createStatement().execute("create table \"QuoteIdentifer\"(\"a\" int default 5)");
		ResultSet rs = con.createStatement().executeQuery("SELECT tbl.* from \"QuoteIdentifer\" tbl");
		assertEquals( "a", rs.getMetaData().getColumnName(1));
		assertEquals( "QuoteIdentifer", rs.getMetaData().getTableName(1));
		while(rs.next()){
            // scroll the result, if there occur an exception
		}
		dropTable(con,"QuoteIdentifer");
	}
}
