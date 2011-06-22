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
 * TestGroupBy.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.junit;

import java.math.BigDecimal;
import java.sql.*;


/**
 * @author Volker Berlin
 *
 */
public class TestGroupBy extends BasicTestCase {

	private static final String table1 = "table_GroupBy1";
	
	private static final String STR_VALUE1 = "name1";
	private static final String STR_VALUE2 = "name2";

	private boolean init;
	public TestGroupBy() {
		super();
	}

	public TestGroupBy(String name) {
		super(name);
	}

	public void init(){
		if(init) return;
		try{
			Connection con = AllTests.getConnection();
			dropTable( con, table1 );
			Statement st = con.createStatement();
			st.execute("create table " + table1 + "(name varchar(30), id int )");
			//st.execute("create table " + table2 + "(c " + testValue.dataType +", d " + testValue.dataType + ")");
			//st.execute("create table " + table3 + "(c " + testValue.dataType +", d " + testValue.dataType + ")");
			st.close();
			PreparedStatement pr = con.prepareStatement("INSERT into " + table1 + "(name, id) Values(?,?)");
			pr.setString( 1, STR_VALUE1);
			pr.setInt( 2, 1 );
			pr.execute();
			pr.setString( 1, STR_VALUE1);
			pr.setInt( 2, 2 );
			pr.execute();
			pr.setString( 1, STR_VALUE1);
			pr.setNull( 2, Types.INTEGER );
			pr.execute();
			pr.setString( 1, STR_VALUE2);
			pr.setInt( 2, 1 );
			pr.execute();

			pr.close();

			init = true;
		}catch(Throwable e){
			e.printStackTrace();
		}
	}

	public void testTest() throws Exception{
		init();
		Connection con = AllTests.getConnection();
		Statement st = con.createStatement();
		ResultSet rs;
		/*
		rs = st.executeQuery("Select name FROM " + table1 + " Group By name");
		while(rs.next()){
			System.out.println( rs.getObject(1) ); 
		}*/
		
		rs = st.executeQuery("Select count(id) FROM " + table1 + " Group By name");
		while(rs.next()){
			rs.getObject(1);
		}

		rs = st.executeQuery("Select count(*) FROM " + table1 + " Group By name");
		while(rs.next()){
			rs.getObject(1);
		}

		rs = st.executeQuery("Select count(*) FROM " + table1);
		assertTrue(rs.next());
		assertEquals( 4, rs.getInt(1));

		rs = st.executeQuery("Select count(id) FROM " + table1);
		assertTrue(rs.next());
		assertEquals( 3, rs.getInt(1));

		rs = st.executeQuery("Select count(*)+1 FROM " + table1);
		assertTrue(rs.next());
		assertEquals( 5, rs.getInt(1));
	}

	public void testCountWhere() throws Exception{
		init();
		assertEqualsRsValue( new Integer(0), "Select count(*) FROM " + table1 + " Where id=-1234");
	}
	
	public void testCountWherePrepare() throws Exception{
		init();
		Connection con = AllTests.getConnection();
		PreparedStatement pr = con.prepareStatement("Select count(*) FROM " + table1 + " Where id=-1234");
		for(int i=1; i<=3; i++){
			ResultSet rs = pr.executeQuery();
			assertTrue( "No row produce in loop:"+i, rs.next());	
			assertEquals( "loop:"+i, 0, rs.getInt(1));
		}
	}
	
	public void testCountOrderBy() throws Exception{
		init();
		Connection con = AllTests.getConnection();
		PreparedStatement pr = con.prepareStatement("Select count(*) FROM " + table1 + " Group By name Order By name DESC");
		for(int i=1; i<=3; i++){
			ResultSet rs = pr.executeQuery( );
			assertTrue  ( "loop:"+i, rs.next());
			assertEquals( "loop:"+i, 1, rs.getInt(1));
			assertTrue  ( "loop:"+i, rs.next());
			assertEquals( "loop:"+i, 3, rs.getInt(1));
		}
	}
	
	public void testGroupByWithExpression() throws Exception{
		init();
		Connection con = AllTests.getConnection();
		PreparedStatement pr = con.prepareStatement("Select sum(id), name+'a' as ColumnName FROM " + table1 + " Group By name+'a' Order By Name+'a'");
		for(int i=1; i<=3; i++){
			ResultSet rs = pr.executeQuery( );
			assertTrue  ( "loop:"+i, rs.next());
			assertEquals( "loop:"+i, 3, rs.getInt(1));
			assertTrue  ( "loop:"+i, rs.next());
			assertEquals( "loop:"+i, 1, rs.getInt(1));
			assertEquals( "loop:"+i+" Alias name von Expression", "ColumnName", rs.getMetaData().getColumnName(2));
		}
	}
	
	public void testComplex() throws Exception{
		init();
		Connection con = AllTests.getConnection();
		PreparedStatement pr = con.prepareStatement("Select abs(sum(abs(3-id))+2) FROM " + table1 + " Group By name+'a' Order By 'b'+(Name+'a')");
		for(int i=1; i<=3; i++){
			ResultSet rs = pr.executeQuery( );
			assertTrue  ( "loop:"+i, rs.next());
			assertEquals( "loop:"+i, 5, rs.getInt(1));
			assertTrue  ( "loop:"+i, rs.next());
			assertEquals( "loop:"+i, 4, rs.getInt(1));
		}
	}
	
	public void testWithNullValue() throws Exception{
		init();
		assertEqualsRsValue(new Integer(4), "Select count(*) FROM " + table1 + " Group By name+null" );
	}
	
	public void testSumInt() throws Exception{
		init();
		assertEqualsRsValue( new Integer(4), "Select sum(id) FROM " + table1);
	}
	
	public void testSumLong() throws Exception{
		init();
		assertEqualsRsValue( new Long(4), "Select sum(cast(id as BigInt)) FROM " + table1);
	}
	
	public void testSumReal() throws Exception{
		init();
		assertEqualsRsValue( new Float(4), "Select sum(cast(id as real)) FROM " + table1);
	}
	
	public void testSumDouble() throws Exception{
		init();
		assertEqualsRsValue( new Double(4), "Select sum(cast(id as double)) FROM " + table1);
	}
	
	public void testSumDecimal() throws Exception{
		init();
		assertEqualsRsValue( new BigDecimal("4.00"), "Select sum(cast(id as decimal(38,2))) FROM " + table1);
	}
	
	public void testMaxInt() throws Exception{
		init();
		assertEqualsRsValue( new Integer(2), "Select max(id) FROM " + table1);
	}
	
	public void testMaxBigInt() throws Exception{
		init();
		assertEqualsRsValue( new Long(2), "Select max(cast(id as BigInt)) FROM " + table1);
	}
	
	public void testMaxString() throws Exception{
		init();
		assertEqualsRsValue( STR_VALUE2, "Select max(name) FROM " + table1);
	}
	
	
	public void testMaxTinyint() throws Exception{
		init();
		assertEqualsRsValue( new Integer(2), "Select max(convert(tinyint,id)) FROM " + table1);
	}
	
	
	public void testMaxReal() throws Exception{
		init();
		assertEqualsRsValue( new Float(2), "Select max(convert(real,id)) FROM " + table1);
	}
	
	
	public void testMaxFloat() throws Exception{
		init();
		assertEqualsRsValue( new Double(2), "Select max(convert(float,id)) FROM " + table1);
	}
	
	
	public void testMaxDouble() throws Exception{
		init();
		assertEqualsRsValue( new Double(2), "Select max(convert(double,id)) FROM " + table1);
	}
	
	
	public void testMaxMoney() throws Exception{
		init();
		assertEqualsRsValue( new java.math.BigDecimal("2.0000"), "Select max(convert(money,id)) FROM " + table1);
	}
	
	
	public void testMaxNumeric() throws Exception{
		init();
		assertEqualsRsValue( new java.math.BigDecimal("2"), "Select max(convert(numeric,id)) FROM " + table1);
	}
	
	
	public void testMaxDate() throws Exception{
		init();
		assertEqualsRsValue( java.sql.Date.valueOf("2345-01-23"), "Select max({d '2345-01-23'}) FROM " + table1);
	}
	
	
	public void testMaxTime() throws Exception{
		init();
		assertEqualsRsValue( java.sql.Time.valueOf("12:34:56"), "Select max({t '12:34:56'}) FROM " + table1);
	}
	
	public void testMaxTimestamp() throws Exception{
		init();
		assertEqualsRsValue( java.sql.Timestamp.valueOf("2345-01-23 12:34:56.123"), "Select max({ts '2345-01-23 12:34:56.123'}) FROM " + table1);
	}
	
	public void testMaxUniqueidentifier() throws Exception{
		init();
		String sql = "Select max(convert(uniqueidentifier, '12345678-3445-3445-3445-1234567890ab')) FROM " + table1;
		assertEqualsRsValue( "12345678-3445-3445-3445-1234567890AB", sql);
	}
	
	public void testMaxOfNull() throws Exception{
		init();
		assertEqualsRsValue( null, "Select max(id) FROM " + table1 + " Where id is null");
	}
	
	public void testMin() throws Exception{
		init();
		assertEqualsRsValue( new Integer(1), "Select min(id) FROM " + table1);
	}
	
	public void testMinString() throws Exception{
		init();
		assertEqualsRsValue( STR_VALUE1, "Select min(name) FROM " + table1);
	}
	
	public void testMinOfNull() throws Exception{
		init();
		assertEqualsRsValue( null, "Select min(id) FROM " + table1 + " Where id is null");
	}
	
	public void testFirst1() throws Exception{
		init();
		assertEqualsRsValue( new Integer(1), "Select first(id) FROM " + table1);
	}
	
	public void testFirst2() throws Exception{
		init();
		assertEqualsRsValue( "name1", "Select first(name) FROM " + table1);
	}
	
	public void testLast1() throws Exception{
		init();
		assertEqualsRsValue( new Integer(1), "Select last(id) FROM " + table1);
	}
	
	public void testLast2() throws Exception{
		init();
		assertEqualsRsValue( "name2", "Select last(name) FROM " + table1);
	}
	
	
	public void testAvg() throws Exception{
		init();
		assertEqualsRsValue( new Integer(1), "Select avg(id) FROM " + table1);
	}
	
	
	public void testGroupBy() throws Exception{
		init();
		Connection con = AllTests.getConnection();
		Statement st = con.createStatement();
		ResultSet rs;
		rs = st.executeQuery("Select name FROM " + table1 + " Group By name");
		assertTrue(rs.next());
		assertEquals( STR_VALUE1, rs.getObject(1) ); 
		assertTrue(rs.next());
		assertEquals( STR_VALUE2, rs.getObject(1) ); 
		
	}
	
	
	/**
	 * A problem can be the metadata from a View.
	 * @throws Exception
	 */
	public void testViewWidthGroupBy() throws Exception{
		init();
		Connection con = AllTests.getConnection();
		Statement st = con.createStatement();
		try{
			ResultSet rs;
			st.execute("Create View qry" + table1 + " as Select name, name as name2, count(*) as count FROM " + table1 + " Group By name");
			rs = st.executeQuery("Select * from qry" + table1);
			assertEquals( "name",  rs.getMetaData().getColumnLabel(1) );
			assertEquals( "name2", rs.getMetaData().getColumnLabel(2) );
			assertEquals( "count", rs.getMetaData().getColumnLabel(3) );
		}finally{
			st.execute("Drop View qry" + table1);
		}
	}
	
	
	public void testCountNoRow() throws Exception{
		init();
	
		// test count(*) without any row
		Connection con = AllTests.getConnection();
		Statement st = con.createStatement();
		st.execute("Delete FROM " + table1);
		init = false;
		assertEqualsRsValue( new Integer(0), "Select count(*) FROM " + table1);
	}
	
}
