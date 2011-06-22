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
 * TestOrderBy.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.junit;

import java.sql.*;
import java.util.ArrayList;

/**
 * @author Administrator
 *
 * To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Generation - Code and Comments
 */
public class TestOrderBy extends BasicTestCase {

	static private boolean init;
	private static final String table1 = "table_OrderBy1";
	private static final String table2 = "table_OrderBy2";
	private static final String table3 = "table_OrderBy3";
	static private int valueCount;
	
	public void init(){
		if(init) return;
		try{
			Connection con = AllTests.getConnection();
			dropTable( con, table1 );
			dropTable( con, table2 );
			dropTable( con, table3 );
			Statement st = con.createStatement();
			st.execute("create table " + table1 + "(v varchar(30), c char(30), nv nvarchar(30),i int, d float, r real, bi bigint, b boolean)");
			st.execute("create table " + table2 + "(c2 char(30))");
			st.execute("create table " + table3 + "(vc varchar(30), vb varbinary(30))");
			st.close();
			
			PreparedStatement pr = con.prepareStatement("INSERT into " + table1 + "(v,c,nv,i,d,r,bi,b) Values(?,?,?,?,?,?,?,?)");
			PreparedStatement pr2= con.prepareStatement("INSERT into " + table2 + "(c2) Values(?)");
			for(int i=150; i>-10; i--){
				pr.setString( 1, String.valueOf(i));
				pr.setString( 2, String.valueOf(i));
				pr.setString( 3, String.valueOf( (char)i ));
				pr.setInt   ( 4, i );
				pr.setDouble( 5, i );
				pr.setFloat ( 6, i );
				pr.setInt   ( 7, i );
				pr.setBoolean( 8, i == 0 );
				pr.execute();
				pr2.setString( 1, String.valueOf(i));
				pr2.execute();
				valueCount++;
			}
			pr.setObject( 1, null, Types.VARCHAR);
			pr.setObject( 2, null, Types.VARCHAR);
			pr.setObject( 3, null, Types.VARCHAR);
			pr.setObject( 4, null, Types.VARCHAR);
			pr.setObject( 5, null, Types.VARCHAR);
			pr.setObject( 6, null, Types.VARCHAR);
			pr.setObject( 7, null, Types.VARCHAR);
			pr.setObject( 8, null, Types.VARCHAR);
			pr.execute();
			pr2.setObject( 1, null, Types.VARCHAR);
			pr2.execute();
			pr2.setString( 1, "");
			pr2.execute();

			pr.close();

			pr = con.prepareStatement("INSERT into " + table3 + "(vc, vb) Values(?,?)");
			pr.setString( 1, table3);
			pr.setBytes( 2, table3.getBytes());
			pr.execute();
			pr.setString( 1, "");
			pr.setBytes( 2, new byte[0]);
			pr.execute();
			pr.setString( 1, null);
			pr.setBytes( 2, null);
			pr.execute();
			
			init = true;
		}catch(Throwable e){
			e.printStackTrace();
		}
	}
	
	
	public void testOrderBy_char() throws Exception{
		init();
		Connection con = AllTests.getConnection();
		Statement st = con.createStatement();
		ResultSet rs;
		String oldValue;
		
		rs = st.executeQuery("SELECT * FROM " + table1 + " ORDER  by c");
		
		assertTrue( rs.next() );
		
		oldValue = rs.getString("c");
		assertNull(oldValue);
		assertTrue( rs.next() );
		oldValue = rs.getString("c");
		
		int count = 1;
		while(rs.next()){
			String newValue = rs.getString("c");
			assertTrue( oldValue + "<" + newValue, oldValue.compareTo( newValue ) < 0 );
			oldValue = newValue;
			count++;
		}
		rs.close();
		assertEquals( valueCount, count );
	}
	

	public void testOrderBy_varchar() throws Exception{
		init();
		Connection con = AllTests.getConnection();
		Statement st = con.createStatement();
		ResultSet rs;
		String oldValue;
		
		rs = st.executeQuery("SELECT * FROM " + table1 + " ORDER  by v");
		
		assertTrue( rs.next() );
		
		oldValue = rs.getString("v");
		assertNull(oldValue);
		assertTrue( rs.next() );
		oldValue = rs.getString("v");
		
		int count = 1;
		while(rs.next()){
			String newValue = rs.getString("v");
			assertTrue( oldValue + "<" + newValue, oldValue.compareTo( newValue ) < 0 );
			oldValue = newValue;
			count++;
		}
		assertEquals( valueCount, count );
	}
	

	public void testOrderBy_varchar_asc() throws Exception{
		init();
		Connection con = AllTests.getConnection();
		Statement st = con.createStatement();
		ResultSet rs;
		String oldValue;
		
		rs = st.executeQuery("SELECT * FROM " + table1 + " ORDER  by v ASC");
		
		assertTrue( rs.next() );
		
		oldValue = rs.getString("v");
		assertNull(oldValue);
		assertTrue( rs.next() );
		oldValue = rs.getString("v");
		
		int count = 1;
		while(rs.next()){
			String newValue = rs.getString("v");
			assertTrue( oldValue.compareTo( newValue ) < 0 );
			oldValue = newValue;
			count++;
		}
		rs.close();
		assertEquals( valueCount, count );
	}
	

	public void testOrderBy_varchar_desc() throws Exception{
		init();
		Connection con = AllTests.getConnection();
		Statement st = con.createStatement();
		ResultSet rs;
		String oldValue;
		
		rs = st.executeQuery("SELECT * FROM " + table1 + " ORDER  by v desc");
		
		assertTrue( rs.next() );
		oldValue = rs.getString("v");
		
		int count = 1;
		while(oldValue != null && rs.next()){
			String newValue = rs.getString("v");
			if(newValue != null){
				assertTrue( oldValue.compareTo( newValue ) > 0 );
				count++;
			}
			oldValue = newValue;
		}
		assertNull(oldValue);
		assertFalse( rs.next() );

		assertEquals( valueCount, count );
	}
	
	
	public void testOrderBy_varchar_DescAsc() throws Exception{
		init();
		Connection con = AllTests.getConnection();
		Statement st = con.createStatement();
		ResultSet rs;
		String oldValue;
		
		rs = st.executeQuery("SELECT * FROM " + table1 + " ORDER  by v desc, i asc");
		
		assertTrue( rs.next() );
		oldValue = rs.getString("v");
		
		int count = 1;
		while(oldValue != null && rs.next()){
			String newValue = rs.getString("v");
			if(newValue != null){
				assertTrue( oldValue.compareTo( newValue ) > 0 );
				count++;
			}
			oldValue = newValue;
		}
		assertNull(oldValue);
		assertFalse( rs.next() );

		assertEquals( valueCount, count );
	}
	
	
	public void testOrderBy_varchar_GroupBy() throws Exception{
		init();
		Connection con = AllTests.getConnection();
		Statement st = con.createStatement();
		ResultSet rs;
		String oldValue;
		
		rs = st.executeQuery("SELECT first(v) cc FROM " + table1 + " Group By i ORDER  by first(V)");
		
		assertTrue( rs.next() );
		
		oldValue = rs.getString("cc");
		assertNull(oldValue);
		assertTrue( rs.next() );
		oldValue = rs.getString("cc");
		
		int count = 1;
		while(rs.next()){
			assertTrue( oldValue.compareTo( rs.getString("cc") ) < 0 );
			oldValue = rs.getString("cc");
			count++;
		}
		assertEquals( valueCount, count );
	}
	

	public void testOrderBy_varchar_Join() throws Exception{
		init();
		Connection con = AllTests.getConnection();
		Statement st = con.createStatement();
		ResultSet rs;
		String oldValue;
		
		rs = st.executeQuery("SELECT * FROM " + table1 + " t1 Inner join "+table2+" t2 on t1.c=t2.c2  ORDER  by v");
		
		assertTrue( rs.next() );
		
		oldValue = rs.getString("v");
		
		int count = 1;
		while(rs.next()){
			assertTrue( oldValue.compareTo( rs.getString("v") ) < 0 );
			oldValue = rs.getString("v");
			count++;
		}
		assertEquals( valueCount, count );
	}
	

	public void testOrderBy_nvarchar() throws Exception{
		init();
		Connection con = AllTests.getConnection();
		Statement st = con.createStatement();
		ResultSet rs;
		String oldValue;
		
		rs = st.executeQuery("SELECT * FROM " + table1 + " ORDER  by nv");
		
		assertTrue( rs.next() );
		
		oldValue = rs.getString("nv");
		assertNull(oldValue);
		assertTrue( rs.next() );
		oldValue = rs.getString("nv");
		
		int count = 1;
		while(rs.next()){
			assertTrue( String.CASE_INSENSITIVE_ORDER.compare( oldValue, rs.getString("nv") ) <= 0 );
			oldValue = rs.getString("nv");
			count++;
		}
		assertEquals( valueCount, count );
	}
	

	public void testOrderBy_int() throws Exception{
		init();
		Connection con = AllTests.getConnection();
		Statement st = con.createStatement();
		ResultSet rs;
		Integer oldValue;
		
		rs = st.executeQuery("SELECT * FROM " + table1 + " ORDER  by i");
		
		assertTrue( rs.next() );
		
		oldValue = (Integer)rs.getObject("i");
		assertNull(oldValue);
		assertTrue( rs.next() );
		oldValue = (Integer)rs.getObject("i");
		
		int count = 1;
		while(rs.next()){
			assertTrue( oldValue.compareTo( (Integer)rs.getObject("i") ) < 0 );
			oldValue = (Integer)rs.getObject("i");
			count++;
		}
		assertEquals( valueCount, count );
	}
	

	public void test_function() throws Exception{
		init();
		Connection con = AllTests.getConnection();
		Statement st = con.createStatement();
		ResultSet rs;
		int oldValue;
		
		rs = st.executeQuery("SELECT * FROM " + table1 + " ORDER  by abs(i)");
		
		assertTrue( rs.next() );
		
		assertNull(rs.getObject("i"));
		assertTrue( rs.next() );
		oldValue = Math.abs( rs.getInt("i") );
		
		int count = 1;
		while(rs.next()){
			int newValue = Math.abs( rs.getInt("i") );
			assertTrue( oldValue <= newValue );
			oldValue = newValue;
			count++;
		}
		assertEquals( valueCount, count );
	}
	

	public void test_functionAscDesc() throws Exception{
		init();
		Connection con = AllTests.getConnection();
		Statement st = con.createStatement();
		ResultSet rs;
		int oldValue;
		int oldValue2;
		
		rs = st.executeQuery("SELECT * FROM " + table1 + " ORDER  by abs(i) Asc, i desc");
		
		assertTrue( rs.next() );
		
		assertNull(rs.getObject("i"));
		assertTrue( rs.next() );
		oldValue = Math.abs( rs.getInt("i") );
		oldValue2 = rs.getInt("i");
		
		int count = 1;
		while(rs.next()){
			int newValue2 = rs.getInt("i");
			int newValue = Math.abs( newValue2 );
			assertTrue( oldValue <= newValue );
			if(oldValue == newValue){
				assertTrue( oldValue2 > newValue2 );
			}
			oldValue = newValue;
			oldValue2 = newValue2;
			count++;
		}
		assertEquals( valueCount, count );
	}
	

	public void testOrderBy_int_asc() throws Exception{
		init();
		Connection con = AllTests.getConnection();
		Statement st = con.createStatement();
		ResultSet rs;
		Integer oldValue;
		
		rs = st.executeQuery("SELECT * FROM " + table1 + " ORDER  by i Asc");
		
		assertTrue( rs.next() );
		
		oldValue = (Integer)rs.getObject("i");
		assertNull(oldValue);
		assertTrue( rs.next() );
		oldValue = (Integer)rs.getObject("i");
		
		int count = 1;
		while(rs.next()){
			assertTrue( oldValue.compareTo( (Integer)rs.getObject("i") ) < 0 );
			oldValue = (Integer)rs.getObject("i");
			count++;
		}
		assertEquals( valueCount, count );
	}
	

	public void testOrderBy_int_desc() throws Exception{
		init();
		Connection con = AllTests.getConnection();
		Statement st = con.createStatement();
		ResultSet rs;
		Integer oldValue;
		
		rs = st.executeQuery("SELECT * FROM " + table1 + " ORDER  by i Desc");
		
		assertTrue( rs.next() );
		oldValue = (Integer)rs.getObject("i");
		
		int count = 1;
		while(oldValue != null && rs.next()){
			Integer newValue = (Integer)rs.getObject("i");
			if(newValue != null){
				assertTrue( oldValue.compareTo( newValue ) > 0 );
				count++;
			}
			oldValue = newValue;
		}
		assertNull(oldValue);
		assertFalse( rs.next() );
		assertEquals( valueCount, count );
	}
	

	public void testOrderBy_double() throws Exception{
		init();
		Connection con = AllTests.getConnection();
		Statement st = con.createStatement();
		ResultSet rs;
		Double oldValue;
		
		rs = st.executeQuery("SELECT * FROM " + table1 + " ORDER  by d");
		
		assertTrue( rs.next() );
		
		oldValue = (Double)rs.getObject("d");
		assertNull(oldValue);
		assertTrue( rs.next() );
		oldValue = (Double)rs.getObject("d");
		
		int count = 1;
		while(rs.next()){
			assertTrue( oldValue.compareTo( (Double)rs.getObject("d") ) < 0 );
			oldValue = (Double)rs.getObject("d");
			count++;
		}
		assertEquals( valueCount, count );
	}
	

	public void testOrderBy_real() throws Exception{
		init();
		Connection con = AllTests.getConnection();
		Statement st = con.createStatement();
		ResultSet rs;
		Float oldValue;
		
		rs = st.executeQuery("SELECT * FROM " + table1 + " ORDER  by r");
		
		assertTrue( rs.next() );
		
		oldValue = (Float)rs.getObject("r");
		assertNull(oldValue);
		assertTrue( rs.next() );
		oldValue = (Float)rs.getObject("r");
		
		int count = 1;
		while(rs.next()){
			assertTrue( oldValue.compareTo( (Float)rs.getObject("r") ) < 0 );
			oldValue = (Float)rs.getObject("r");
			count++;
		}
		assertEquals( valueCount, count );
	}
	

	public void test_bigint() throws Exception{
		init();
		Connection con = AllTests.getConnection();
		Statement st = con.createStatement();
		ResultSet rs;
		Long oldValue;
		
		rs = st.executeQuery("SELECT * FROM " + table1 + " ORDER  by bi");
		
		assertTrue( rs.next() );
		
		oldValue = (Long)rs.getObject("bi");
		assertNull(oldValue);
		assertTrue( rs.next() );
		oldValue = (Long)rs.getObject("bi");
		
		int count = 1;
		while(rs.next()){
			assertTrue( oldValue.compareTo( (Long)rs.getObject("bi") ) < 0 );
			oldValue = (Long)rs.getObject("bi");
			count++;
		}
		assertEquals( valueCount, count );
	}
	

	public void test_bigint_withDoublicateValues() throws Exception{
		init();
		Connection con = AllTests.getConnection();
		Statement st = con.createStatement();
		ResultSet rs;
		Long oldValue;
		
		rs = st.executeQuery("SELECT bi/2 bi_2 FROM " + table1 + " ORDER  by (bi/2)");
		
		assertTrue( rs.next() );
		
		oldValue = (Long)rs.getObject("bi_2");
		assertNull(oldValue);
		assertTrue( rs.next() );
		oldValue = (Long)rs.getObject("bi_2");
		
		int count = 1;
		while(rs.next()){
			Long newValue = (Long)rs.getObject("bi_2");
			assertTrue( oldValue + "<="+newValue, oldValue.compareTo( newValue ) <= 0 );
			oldValue = newValue;
			count++;
		}
		assertEquals( valueCount, count );
	}
	

	public void test_boolean() throws Exception{
		init();
		Connection con = AllTests.getConnection();
		Statement st = con.createStatement();
		ResultSet rs;
		boolean oldValue;
		
		rs = st.executeQuery("SELECT * FROM " + table1 + " ORDER  by b");
		
		assertTrue( rs.next() );
		
		oldValue = rs.getBoolean("b");
		assertFalse(oldValue);
		assertTrue(rs.wasNull());
		assertTrue( rs.next() );
		oldValue = rs.getBoolean("b");
		assertFalse(oldValue);		
		assertFalse(rs.wasNull());
		
		int count = 1;
		while(!oldValue && rs.next()){
			oldValue = rs.getBoolean("b");
			assertFalse(rs.wasNull());
			count++;
		}
		while(oldValue && rs.next()){
			oldValue = rs.getBoolean("b");
			assertFalse(rs.wasNull());
			count++;
		}
		assertFalse(rs.next());
		assertEquals( valueCount, count );
	}
	

	public void testVarcharEmpty() throws Exception{
		init();
		Connection con = AllTests.getConnection();
		Statement st = con.createStatement();
		ResultSet rs;
		
		rs = st.executeQuery("SELECT * FROM " + table3 + " ORDER  by vc");
		
		assertTrue( rs.next() );		
		assertNull( rs.getObject("vc") );

		assertTrue( rs.next() );
		assertEquals( "", rs.getObject("vc") );
		
		assertTrue( rs.next() );
		assertEquals( table3, rs.getObject("vc") );
		
		assertFalse( rs.next() );
	}
	

	public void testVarbinaryEmpty() throws Exception{
		init();
		Connection con = AllTests.getConnection();
		Statement st = con.createStatement();
		ResultSet rs;
		
		rs = st.executeQuery("SELECT * FROM " + table3 + " ORDER  by vb");
		
		assertTrue( rs.next() );		
		assertNull( rs.getObject("vb") );

		assertTrue( rs.next() );
		assertEqualsObject( "", new byte[0], rs.getObject("vb"), false );
		
		assertTrue( rs.next() );
		assertEqualsObject( "", table3.getBytes(), rs.getObject("vb"), false );
		
		assertFalse( rs.next() );
	}


	public void test2Columns() throws Exception{
		init();
		Connection con = AllTests.getConnection();
		Statement st = con.createStatement();
		ResultSet rs = null;
		String oldValue;

		rs = st.executeQuery("SELECT * FROM " + table1+","+table2+" ORDER  by v, c2");

		assertTrue( rs.next() );
		assertNull( rs.getObject("v") );
		assertNull( rs.getObject("c2") );
		
		assertTrue( rs.next() );
		oldValue = rs.getString("c2");

		int count = 1;
		while(rs.next() && rs.getString("v") == null){
			String newValue = rs.getString("c2");
			assertTrue( oldValue.compareTo( newValue ) < 0 );
			oldValue = newValue;
			count++;
		}
		assertEquals( valueCount+1, count );
		
		boolean isNext = true;
		while(isNext){
			String vValue = rs.getString("v");
			assertNull( rs.getObject("c2") );
		
			assertTrue( rs.next() );
			oldValue = rs.getString("c2");
			assertEquals( vValue, rs.getString("v") );

			isNext = rs.next();
			count = 1;
			while(isNext && vValue.equals(rs.getString("v"))){
				String newValue = rs.getString("c2");
				assertTrue( oldValue.compareTo( newValue ) < 0 );
				oldValue = newValue;
				count++;
				isNext = rs.next();
			}
			assertEquals( valueCount+1, count );
		}
	}

	

	public void testOrderBy_Scollable() throws Exception{
		init();
		Connection con = AllTests.getConnection();
		Statement st = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
		ResultSet rs;
		int count;
		
		rs = st.executeQuery("SELECT * FROM " + table1 + " ORDER  by v");
		
		//jetzt irgendwo in die Mitte scrollen
		rs.next();
		rs.next();
		rs.previous(); //dann soll der Zeiger nicht am Ende des bereits gefetchten stehen
		
		rs.last();
		count = 0;
		while(rs.previous()) count++;		
		assertEquals( valueCount, count );

		rs.beforeFirst();
		count = -1;
		while(rs.next()) count++;		
		assertEquals( valueCount, count );

		rs.beforeFirst();
		count = -1;
		while(rs.next()) count++;		
		assertEquals( valueCount, count );
	}

	
	public void testOrderBy_ScollableDesc() throws Exception{
		init();
		Connection con = AllTests.getConnection();
		Statement st = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
		ResultSet rs;
		int count;
		
		rs = st.executeQuery("SELECT * FROM " + table1 + " ORDER  by i desc, d");
		
		//jetzt irgendwo in die Mitte scrollen
		rs.next();
		rs.next();
		rs.previous(); //dann soll der Zeiger nicht am Ende des bereits gefetchten stehen
		
		rs.last();
		count = 0;
		while(rs.previous()) count++;		
		assertEquals( valueCount, count );

		rs.beforeFirst();
		count = -1;
		while(rs.next()) count++;		
		assertEquals( valueCount, count );

		rs.beforeFirst();
		count = -1;
		while(rs.next()) count++;		
		assertEquals( valueCount, count );
	}

	
	public void testOrderBy_Scollable2() throws Exception{
		init();
		Connection con = AllTests.getConnection();
		Statement st = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
		ResultSet rs = st.executeQuery("SELECT * FROM " + table1 + " ORDER  by v");

		
		int colCount = rs.getMetaData().getColumnCount();
		ArrayList result = new ArrayList();
		while(rs.next()){
			Object[] row = new Object[colCount];
			for(int i=0; i<colCount; i++){
				row[i] = rs.getObject(i+1);
			}
			result.add(row);
		}
		
		int rowCount = result.size();
		while(rs.previous()){
			Object[] row = (Object[])result.get(--rowCount);
			for(int i=0; i<colCount; i++){
				assertEquals( "Difference in row:"+rowCount, row[i], rs.getObject(i+1));
			}
		}
		assertEquals( "RowCount different between next and previous:"+rowCount, 0, rowCount);
	}

	
	public void testUnion() throws Exception{
		init();
		Connection con = AllTests.getConnection();
		Statement st = con.createStatement();
		ResultSet rs;
		String oldValue;
		
		rs = st.executeQuery("SELECT v, 5 as Const FROM " + table1 + " Union All Select vc, 6 From " + table3 + " ORDER by v");
		
		assertRSMetaData(rs, new String[]{"v", "Const"}, new int[]{Types.VARCHAR, Types.INTEGER});
		
		assertTrue( rs.next() );		
		oldValue = rs.getString("v");
		assertNull(oldValue);
		
		assertTrue( rs.next() );		
		oldValue = rs.getString("v");
		assertNull(oldValue);
		
		assertTrue( rs.next() );
		oldValue = rs.getString("v");
		
		int count = 3;
		while(rs.next()){
			String newValue = rs.getString("v");
			assertTrue( oldValue.compareTo( newValue ) < 0 );
			oldValue = newValue;
			count++;
		}
		assertEquals( valueCount+4, count );
	}


}
