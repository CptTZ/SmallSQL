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
 * TestStatement.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 23.04.2006
 */
package smallsql.junit;

import java.sql.*;


/**
 * 
 * @author Volker Berlin
 */
public class TestStatement extends BasicTestCase {

	private static boolean init;

    
    protected void setUp() throws Exception{
    	if(init) return;
		Connection con = AllTests.getConnection();
		Statement st = con.createStatement();
		dropTable( con, "statement");
		st.execute("Create Table statement (c varchar(30), i counter)");
		init = true;
    }
    
    
    public void testBatchUpate() throws Exception{
		Connection con = AllTests.getConnection();
		Statement st = con.createStatement();
		
		assertEquals("Result Length wrong", 0, st.executeBatch().length );
		st.clearBatch();
		st.addBatch("Bla Bla");
        try {
            st.executeBatch();
        } catch (BatchUpdateException ex) {
            assertEquals("Result Length wrong",1,ex.getUpdateCounts().length);
        }
		st.clearBatch();
		int count = 10;
		for(int i=1; i<=count; i++){
			st.addBatch("Insert Into statement(c) Values('batch"+i+"')");
		}
		int[] result = st.executeBatch();
		assertEquals("Result Length wrong", count, result.length);
		for(int i=0; i<count; i++){
			assertEquals("Update Count", 1, result[i]);
		}
		assertRowCount(10, "Select * From statement");
    }
    
    
    public void testMultiValues() throws Exception{
		Connection con = AllTests.getConnection();
		Statement st = con.createStatement();
    	assertEquals("Update Count:", 10, st.executeUpdate("Insert Into statement(c) Values('abc1'),('abc2'),('abc3'),('abc4'),('abc5'),('abc6'),('abc7'),('abc8'),('abc9'),('abc10')"));
    }
    

    public void testMaxRows() throws Exception{
        Connection con = AllTests.getConnection();
        Statement st = con.createStatement();
        st.setMaxRows(5);
        ResultSet rs = st.executeQuery("Select * From statement");
        assertEquals("Statement.getResultSet", rs, st.getResultSet());
        assertRowCount(5,rs);
        assertRowCount(4,"Select top 4 * From statement");
        assertRowCount(3,"Select * From statement Limit 3");
        assertRowCount(2,"Select * From statement Order By c ASC Limit 2");
        assertRowCount(0,"Select top 0 * From statement");
        
        st = con.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE );
        rs = st.executeQuery("Select Top 0 * From statement");
        assertFalse( "last()", rs.last() );
        
        PreparedStatement pr = con.prepareStatement("Select * From statement");
        pr.setMaxRows(6);
        rs = pr.executeQuery();
        assertEquals("PreparedStatement.getResultSet", rs, pr.getResultSet());
        assertRowCount(6,rs);
        
        pr.setMaxRows(3);
        rs = pr.executeQuery();
        assertRowCount(3,rs);
               
        pr.setMaxRows(4);
        rs = pr.executeQuery();
        assertRowCount(4,rs);
    }
    
    
    public void testMoreResults() throws Exception{
        Connection con = AllTests.getConnection();
        Statement st = con.createStatement();
        
        ResultSet rs = st.executeQuery("Select * From statement");
        assertEquals( "getResultSet()", rs, st.getResultSet() );
        assertEquals( "getUpdateCount()", -1, st.getUpdateCount() );
        assertFalse( st.getMoreResults() );
        try{
            rs.next();
            fail("ResultSet should be closed");
        }catch(SQLException ex){
            assertSQLException("01000", 0, ex);
        }
        assertNull( "getResultSet()", st.getResultSet() );
        assertEquals( "getUpdateCount()", -1, st.getUpdateCount() );
        
        
        rs = st.executeQuery("Select * From statement");
        assertEquals( "getResultSet()", rs, st.getResultSet() );
        assertEquals( "getUpdateCount()", -1, st.getUpdateCount() );
        assertFalse( st.getMoreResults(Statement.KEEP_CURRENT_RESULT) );
        assertTrue(rs.next());
        assertNull( "getResultSet()", st.getResultSet() );
        assertEquals( "getUpdateCount()", -1, st.getUpdateCount() );
        
        
        int count = st.executeUpdate("Update statement set c = c");
        assertTrue( "Update Erfolgreich", count>0 );
        assertNull( "getResultSet()", st.getResultSet() );
        assertEquals( "getUpdateCount()", count, st.getUpdateCount() );
        assertFalse( st.getMoreResults() );
        assertNull( "getResultSet()", st.getResultSet() );
        assertEquals( "getUpdateCount()", -1, st.getUpdateCount() );
    }
    
    
    public void testGetConnection() throws Exception{
        Connection con = AllTests.getConnection();
        Statement st = con.createStatement();
        
        assertEquals(con, st.getConnection() );
    }
    
    
    public void testFetch() throws Exception{
        Connection con = AllTests.getConnection();
        Statement st = con.createStatement();
        
        st.setFetchDirection(ResultSet.FETCH_FORWARD);
        assertEquals( st.getFetchDirection(), ResultSet.FETCH_FORWARD);

        st.setFetchDirection(ResultSet.FETCH_REVERSE);
        assertEquals( st.getFetchDirection(), ResultSet.FETCH_REVERSE);
        
        st.setFetchSize(123);
        assertEquals( st.getFetchSize(), 123);
    }
    
    
    public void testGeneratedKeys() throws Exception{
        Connection con = AllTests.getConnection();
        Statement st = con.createStatement();
        ResultSet rs;
        
        st.execute("Insert Into statement(c) Values('key1')", Statement.NO_GENERATED_KEYS);
        try{
            st.getGeneratedKeys();
            fail("NO_GENERATED_KEYS");
        }catch(SQLException ex){
            assertSQLException("01000", 0, ex);
        }
        assertEquals("UpdateCount", 1, st.getUpdateCount());
        assertNull("getResultSet", st.getResultSet());
        
        st.execute("Insert Into statement(c) Values('key2')", Statement.RETURN_GENERATED_KEYS);
        rs = st.getGeneratedKeys();
        assertNotNull("RETURN_GENERATED_KEYS", rs);
        assertEquals("ColumnCount",1,rs.getMetaData().getColumnCount());
        assertEquals("ColumnCount","i",rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
        assertEqualsRsValue( new Long(rs.getLong(1)), rs, false );
        assertFalse(rs.next());


        assertEquals(1,st.executeUpdate("Insert Into statement(c) Values('key3')", Statement.RETURN_GENERATED_KEYS));
        rs = st.getGeneratedKeys();
        assertNotNull("RETURN_GENERATED_KEYS", rs);
        assertEquals("ColumnCount",1,rs.getMetaData().getColumnCount());
        assertEquals("ColumnCount","i",rs.getMetaData().getColumnName(1));
        assertRowCount(1,rs);

        st.execute("Insert Into statement(c) Values('key4')", new int[]{2,1});
        rs = st.getGeneratedKeys();
        assertNotNull("RETURN_GENERATED_KEYS", rs);
        assertEquals("ColumnCount",2,rs.getMetaData().getColumnCount());
        assertRowCount(1,rs);

        assertEquals(1,st.executeUpdate("Insert Into statement(c) Values('key5')", new int[]{2}));
        rs = st.getGeneratedKeys();
        assertNotNull("RETURN_GENERATED_KEYS", rs);
        assertEquals("ColumnCount",1,rs.getMetaData().getColumnCount());
        assertEquals("ColumnCount","i",rs.getMetaData().getColumnName(1));
        assertRowCount(1,rs);

        st.execute("Insert Into statement(c) Values('key6')", new String[]{"c","i"});
        rs = st.getGeneratedKeys();
        assertNotNull("RETURN_GENERATED_KEYS", rs);
        assertEquals("ColumnCount",2,rs.getMetaData().getColumnCount());
        assertRowCount(1,rs);

        assertEquals(1,st.executeUpdate("Insert Into statement(c) Values('key7')", new String[]{"i"}));
        rs = st.getGeneratedKeys();
        assertNotNull("RETURN_GENERATED_KEYS", rs);
        assertEquals("ColumnCount",1,rs.getMetaData().getColumnCount());
        assertEquals("ColumnCount","i",rs.getMetaData().getColumnName(1));
        assertRowCount(1,rs);
    }
    
    
    public void testGeneratedKeysWithPrepare() throws Exception{
        Connection con = AllTests.getConnection();
        ResultSet rs;
        
        PreparedStatement pr = con.prepareStatement("Insert Into statement(c) Values('key1')", Statement.NO_GENERATED_KEYS);
        pr.execute();
        try{
            pr.getGeneratedKeys();
            fail("NO_GENERATED_KEYS");
        }catch(SQLException ex){
            assertSQLException("01000", 0, ex);
        }
        assertEquals("UpdateCount", 1, pr.getUpdateCount());
        assertNull("getResultSet", pr.getResultSet());
        pr.close();
        
        pr = con.prepareStatement("Insert Into statement(c) Values('key2')", Statement.RETURN_GENERATED_KEYS);
        pr.execute();
        rs = pr.getGeneratedKeys();
        assertNotNull("RETURN_GENERATED_KEYS", rs);
        assertEquals("ColumnCount",1,rs.getMetaData().getColumnCount());
        assertEquals("ColumnCount","i",rs.getMetaData().getColumnName(1));
        assertRowCount(1,rs);

        pr = con.prepareStatement("Insert Into statement(c) Values('key3')", Statement.RETURN_GENERATED_KEYS);
        assertEquals(1,pr.executeUpdate());
        rs = pr.getGeneratedKeys();
        assertNotNull("RETURN_GENERATED_KEYS", rs);
        assertEquals("ColumnCount",1,rs.getMetaData().getColumnCount());
        assertEquals("ColumnCount","i",rs.getMetaData().getColumnName(1));
        assertRowCount(1,rs);

        pr = con.prepareStatement("Insert Into statement(c) Values('key4')", new int[]{2,1});
        pr.execute();
        rs = pr.getGeneratedKeys();
        assertNotNull("RETURN_GENERATED_KEYS", rs);
        assertEquals("ColumnCount",2,rs.getMetaData().getColumnCount());
        assertRowCount(1,rs);

        pr = con.prepareStatement("Insert Into statement(c) Values('key5')", new int[]{2});
        assertEquals(1,pr.executeUpdate());
        rs = pr.getGeneratedKeys();
        assertNotNull("RETURN_GENERATED_KEYS", rs);
        assertEquals("ColumnCount",1,rs.getMetaData().getColumnCount());
        assertEquals("ColumnCount","i",rs.getMetaData().getColumnName(1));
        assertRowCount(1,rs);

        pr = con.prepareStatement("Insert Into statement(c) Values('key6')", new String[]{"c","i"});
        pr.execute();
        rs = pr.getGeneratedKeys();
        assertNotNull("RETURN_GENERATED_KEYS", rs);
        assertEquals("ColumnCount",2,rs.getMetaData().getColumnCount());
        assertRowCount(1,rs);

        pr = con.prepareStatement("Insert Into statement(c) Values('key7')", new String[]{"i"});
        assertEquals(1,pr.executeUpdate());
        rs = pr.getGeneratedKeys();
        assertNotNull("RETURN_GENERATED_KEYS", rs);
        assertEquals("ColumnCount",1,rs.getMetaData().getColumnCount());
        assertEquals("ColumnCount","i",rs.getMetaData().getColumnName(1));
        assertRowCount(1,rs);
    }
    
    
    public void testResultSetType() throws Exception{
        Connection con = AllTests.getConnection();
        Statement st = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        
        assertEquals(ResultSet.TYPE_SCROLL_SENSITIVE, st.getResultSetType());
        assertEquals(ResultSet.CONCUR_UPDATABLE, st.getResultSetConcurrency());
        
        ResultSet rs = st.executeQuery("Select * From statement");
        
        assertEquals(ResultSet.TYPE_SCROLL_SENSITIVE, rs.getType());
        assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
    }

    
    public void testOther() throws Exception{
        //now we test all not implemented code
        Connection con = AllTests.getConnection();
        Statement st = con.createStatement();
        
        //curently there are no known warnings
        st.clearWarnings();
        assertNull(st.getWarnings());
        
        //query execution does not need any time the time occur on next,
        //but currently there is no time observer
        st.setQueryTimeout(5);
        assertEquals("QueryTimeout", 5, st.getQueryTimeout() );
        
        st.setMaxFieldSize(100);
        assertEquals("MaxFieldSize", 100, st.getMaxFieldSize() );
    }
    
    
    public void testTruncate() throws Exception{
        Connection con = AllTests.getConnection();
        Statement st = con.createStatement();
        
        st.execute("Truncate table statement");
        assertRowCount(0, "Select * From statement");
    }
}