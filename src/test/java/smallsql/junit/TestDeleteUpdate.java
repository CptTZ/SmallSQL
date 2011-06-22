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
 * TestDeleteUpdate.java
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
public class TestDeleteUpdate extends BasicTestCase {

	/**
	 * 
	 */
	public TestDeleteUpdate() {
		super();
	}

	/**
	 * @param name
	 */
	public TestDeleteUpdate(String name) {
		super(name);
	}


	public void testDelete() throws Exception{
		Connection con = AllTests.getConnection();
		dropTable(con,"testDelete");
		Statement st = con.createStatement();
		st.execute("create table testDelete(a int default 15)");
		for(int i=0; i<10; i++){
			st.execute("Insert into testDelete Values("+i+")");
		}
		assertRowCount( 10, "Select * from testDelete");
		
		st.execute("delete from testDelete Where a=3");
		assertRowCount( 9, "Select * from testDelete");
		
		st.execute("delete from testDelete Where a<5");
		assertRowCount( 5, "Select * from testDelete");
		
		st.execute("delete from testDelete");
		assertRowCount( 0, "Select * from testDelete");
		dropTable(con,"testDelete");
	}
	
	public void testUpdate1() throws Exception{
		Connection con = AllTests.getConnection();
		dropTable(con,"testUpdate");
		Statement st = con.createStatement();
		st.execute("create table testUpdate(id int default 15, value int)");
		for(int i=0; i<10; i++){
			st.execute("Insert into testUpdate Values("+i+','+i+")");
		}
		assertRowCount( 10, "Select * from testUpdate");
		
		int updateCount;
		updateCount = st.executeUpdate("update testUpdate set value=103 Where id=3");
		assertEqualsRsValue( new Integer(103), "Select value from testUpdate Where id=3");
		assertRowCount( 10, "Select value from testUpdate");
		assertEquals( 1, updateCount);
		
		updateCount = st.executeUpdate("update testUpdate set value=104 Where id=3");
		assertEqualsRsValue( new Integer(104), "Select value from testUpdate Where id=3");
		assertRowCount( 10, "Select value from testUpdate");
		assertEquals( 1, updateCount);
		
		updateCount = st.executeUpdate("delete from testUpdate Where id=3");
		assertRowCount( 9, "Select * from testUpdate");
		assertEquals( 1, updateCount);

		updateCount = st.executeUpdate("update testUpdate set value=27 Where id<5");
		assertEquals( 4, updateCount);

		dropTable(con,"testUpdate");
	}
	
	public void testUpdate2() throws Exception{
		Connection con = AllTests.getConnection();
		dropTable(con,"testUpdate");
		Statement st = con.createStatement();
		st.execute("create table testUpdate(id int default 15, value1 varchar(100), value2 int)");
		for(int i=0; i<10; i++){
			st.execute("Insert into testUpdate Values("+i+','+(i*100)+','+i+")");
		}
		assertRowCount( 10, "Select * from testUpdate");
		
		st.execute("update testUpdate set value1=13 Where id=3");
		assertEqualsRsValue( "13", "Select value1 from testUpdate Where id=3");
		assertRowCount( 10, "Select * from testUpdate");
		
		st.execute("update testUpdate set value1=1040 Where id=3");
		assertEqualsRsValue( "1040", "Select value1 from testUpdate Where id=3");
		assertRowCount( 10, "Select * from testUpdate");
		
		st.execute("update testUpdate set value1=10400 Where id=3");
		assertEqualsRsValue( "10400", "Select value1 from testUpdate Where id=3");
		assertRowCount( 10, "Select * from testUpdate");
		
		st.execute("update testUpdate set value1=13,id=3 Where id=3");
		assertEqualsRsValue( "13", "Select value1 from testUpdate Where id=3");
		assertRowCount( 10, "Select * from testUpdate");
		
		st.execute("delete from testUpdate Where id=3");
		assertRowCount( 9, "Select * from testUpdate");

		dropTable(con,"testUpdate");
	}
	

	public void testUpdateMultiTables() throws Exception{
		Connection con = AllTests.getConnection();
		dropTable(con,"testUpdate1");
		dropTable(con,"testUpdate2");
		Statement st = con.createStatement();
		st.execute("create table testUpdate1(id1 int, value1 varchar(100))");
		st.execute("create table testUpdate2(id2 int, value2 varchar(100))");
		st.execute("Insert into testUpdate1 Values(11, 'qwert1')");
		st.execute("Insert into testUpdate2 Values(11, 'qwert2')");
		
		st.execute("update testUpdate1 inner join testUpdate2 on id1=id2 Set value1=value1+'update', value2=value2+'update'");
		ResultSet rs = st.executeQuery("Select * From testUpdate1 inner join testUpdate2 on id1=id2");
		assertTrue( rs.next() );
		assertEquals( "qwert1update", rs.getString("value1"));
		assertEquals( "qwert2update", rs.getString("value2"));

		dropTable(con,"testUpdate1");
		dropTable(con,"testUpdate2");
	}
}
