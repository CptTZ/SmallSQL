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
 * TestResultSet.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 01.05.2006
 */
package smallsql.junit;

import java.sql.*;


/**
 * 
 * @author Volker Berlin
 */
public class TestResultSet extends BasicTestCase {

	private static boolean init;

    
    protected void setUp() throws Exception{
    	if(init) return;
		Connection con = AllTests.getConnection();
		Statement st = con.createStatement();
		dropTable( con, "ResultSet");
		st.execute("Create Table ResultSet (i int identity, c varchar(30))");
        
        st = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = st.executeQuery("Select * From ResultSet");
        
        rs.moveToInsertRow();
        rs.insertRow();
        rs.moveToInsertRow();
        rs.insertRow();
		init = true;
    }


    public void testScrollStates() throws Exception{
        Connection con = AllTests.getConnection();
        Statement st = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = st.executeQuery("Select * From ResultSet Where 1=0");
        
        assertTrue("isBeforeFirst", rs.isBeforeFirst() );
        assertTrue("isAfterLast", rs.isAfterLast() );
        assertEquals("getRow", 0, rs.getRow() );
        
        rs.moveToInsertRow();
        rs.insertRow();
        
        rs.beforeFirst();
        assertTrue("isBeforeFirst", rs.isBeforeFirst() );
        assertFalse("isAfterLast", rs.isAfterLast() );
        assertEquals("getRow", 0, rs.getRow() );
        assertTrue("next", rs.next() );
        assertTrue("isFirst", rs.isFirst() );
        assertTrue("rowInserted", rs.rowInserted() );
        assertEquals("getRow", 1, rs.getRow() );
        assertTrue("isLast", rs.isLast() );
        assertFalse("next", rs.next() );
        assertFalse("isBeforeFirst", rs.isBeforeFirst() );
        assertTrue("isAfterLast", rs.isAfterLast() );
        assertEquals("getRow", 0, rs.getRow() );
        
        assertTrue("first", rs.first() );
        assertEquals("getRow", 1, rs.getRow() );
        
        assertFalse("previous", rs.previous() );
        assertEquals("getRow", 0, rs.getRow() );
        assertTrue("isBeforeFirst", rs.isBeforeFirst() );
        assertFalse("isAfterLast", rs.isAfterLast() );
        
        assertTrue("last", rs.last() );
        assertEquals("getRow", 1, rs.getRow() );
        assertTrue("isLast", rs.isLast() );
        
        rs.afterLast();
        assertFalse("isBeforeFirst", rs.isBeforeFirst() );
        assertTrue("isAfterLast", rs.isAfterLast() );
        assertEquals("getRow", 0, rs.getRow() );
    }
    

    public void testScrollStatesGroupBy() throws Exception{
        Connection con = AllTests.getConnection();
        Statement st = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = st.executeQuery("Select i,max(c) From ResultSet Group By i HAVING i=1");
        
        assertEquals("getConcurrency",ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
        assertTrue("isBeforeFirst", rs.isBeforeFirst() );
        assertFalse("isAfterLast", rs.isAfterLast() );
        assertEquals("getRow", 0, rs.getRow() );
        
        rs.beforeFirst();
        assertTrue("isBeforeFirst", rs.isBeforeFirst() );
        assertFalse("isAfterLast", rs.isAfterLast() );
        assertEquals("getRow", 0, rs.getRow() );
        assertTrue("next", rs.next() );
        assertTrue("isFirst", rs.isFirst() );
        assertFalse("rowInserted", rs.rowInserted() );
        assertEquals("getRow", 1, rs.getRow() );
        assertTrue("isLast", rs.isLast() );
        assertFalse("next", rs.next() );
        assertFalse("isBeforeFirst", rs.isBeforeFirst() );
        assertTrue("isAfterLast", rs.isAfterLast() );
        assertEquals("getRow", 0, rs.getRow() );
        
        assertTrue("first", rs.first() );
        assertEquals("getRow", 1, rs.getRow() );
        
        assertFalse("previous", rs.previous() );
        assertEquals("getRow", 0, rs.getRow() );
        assertTrue("isBeforeFirst", rs.isBeforeFirst() );
        assertFalse("isAfterLast", rs.isAfterLast() );
        
        assertTrue("last", rs.last() );
        assertEquals("getRow", 1, rs.getRow() );
        assertTrue("isLast", rs.isLast() );
        
        rs.afterLast();
        assertFalse("isBeforeFirst", rs.isBeforeFirst() );
        assertTrue("isAfterLast", rs.isAfterLast() );
        assertEquals("getRow", 0, rs.getRow() );
    }
    

    public void testUpdate() throws Exception{
        Connection con = AllTests.getConnection();
        Statement st = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs;
        
        
        // first test with a enlarging row size
        rs = st.executeQuery("Select * From ResultSet");
        assertTrue("next", rs.next());
        assertEquals("getRow", 1, rs.getRow() );
        int id = rs.getInt("i");
        rs.updateShort("c", (short)123 );
        assertEquals( (short)123, rs.getShort("c") );
        assertEquals( id, rs.getInt("i") ); //check a not updated row
        rs.updateRow();
        assertEquals( (short)123, rs.getShort("c") );
        assertFalse( rs.rowUpdated() );  //false because currently it is not implemented and we does not plan it
        assertFalse( rs.rowInserted() );
        assertFalse( rs.rowDeleted() );
        assertEquals("getRow", 1, rs.getRow() );
        
        // second test with a reduce row size
        rs = st.executeQuery("Select * From ResultSet");
        assertTrue("next", rs.next());
        rs.updateByte("c", (byte)66 );
        assertEquals( (byte)66, rs.getByte("c") );
        rs.updateRow();
        assertEquals( (short)66, rs.getShort("c") );
    }
    
    
    /**
     * test if scrolling reset the update values
     */
    public void testUpdateAndScroll() throws Exception{
        final Object value = "UpdateAndScroll";
        Object value1;
        Object value2;
        Connection con = AllTests.getConnection();
        Statement st = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = st.executeQuery("Select * From ResultSet");
        
        //method first
        assertTrue("start", rs.last());
        value1 = rs.getObject("i");
        rs.updateObject("c", value, Types.VARCHAR );
        assertEquals("getObject", value, rs.getObject("c"));
        assertEquals("getObject", value1, rs.getObject("i"));
        assertTrue("first", rs.first());
        assertNotSame("getObject", value, rs.getObject("c"));
        
        //method next
        assertTrue("start", rs.first());
        rs.updateObject("c", value, Types.VARCHAR );
        assertEquals("getObject", value, rs.getObject("c"));
        assertTrue("next", rs.next());
        assertNotSame("getObject", value, rs.getObject("c"));
        
        //method previous
        assertTrue("start", rs.last());
        rs.updateObject("c", value );
        assertEquals("getObject", value, rs.getObject("c"));
        assertTrue("previous", rs.previous());
        assertNotSame("getObject", value, rs.getObject("c"));
        
        //method last
        assertTrue("start", rs.first());
        rs.updateObject("c", value, Types.VARCHAR );
        assertEquals("getObject", value, rs.getObject("c"));
        assertTrue("last", rs.last());
        assertNotSame("getObject", value, rs.getObject("c"));
        
        //method refresh
        assertTrue("start", rs.first());
        rs.updateObject("c", value, Types.VARCHAR );
        assertEquals("getObject", value, rs.getObject("c"));
        rs.refreshRow();
        assertNotSame("getObject", value, rs.getObject("c"));
        
        //method moveToInsertRow and moveToCurrentRow
        assertTrue("start", rs.first());
        value1 = rs.getObject("i");
        value2 = rs.getObject("c");
        rs.updateObject("c", value);
        assertEquals("getObject", value, rs.getObject("c"));
        rs.moveToInsertRow();
        assertNull("new row", rs.getObject("i"));
        assertNull("new row", rs.getObject("c"));
        rs.updateObject("c", value);
        assertEquals("getObject", value, rs.getObject("c"));
        rs.moveToCurrentRow();
        assertEquals("getObject", value1, rs.getObject("i"));
        assertEquals("getObject", value2, rs.getObject("c"));
    }
    
    
    public void testDelete() throws Exception{
        Connection con = AllTests.getConnection();
        Statement st = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = st.executeQuery("Select * From ResultSet Where i>1");
        
        assertTrue("next", rs.next());
        assertFalse( rs.rowDeleted() );
        rs.deleteRow();
        assertTrue( rs.rowDeleted() );
    }
    
    
    public void testOther() throws Exception{
        Connection con = AllTests.getConnection();
        Statement st = con.createStatement();
        ResultSet rs = st.executeQuery("Select * From ResultSet");
        
        assertEquals(st, rs.getStatement());
        
        //currently there are no known warnings
        rs.clearWarnings();
        assertNull(rs.getWarnings());
        
        rs.setFetchDirection(ResultSet.FETCH_FORWARD);
        assertEquals( rs.getFetchDirection(), ResultSet.FETCH_FORWARD);

        rs.setFetchDirection(ResultSet.FETCH_REVERSE);
        assertEquals( rs.getFetchDirection(), ResultSet.FETCH_REVERSE);
        
        rs.setFetchSize(123);
        assertEquals( rs.getFetchSize(), 123);
    }
}