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
 * TestScrollable.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 14.08.2004
 */
package smallsql.junit;

import java.sql.*;

/**
 * @author Volker Berlin
 */
public class TestScrollable extends BasicTestCase {
	
	public void testLastWithWhere() throws Exception{
		Connection con = AllTests.getConnection();
		try{			
			con.createStatement().execute("Create Table Scrollable (i counter, v varchar(20))");
			assertRowCount( 0, "Select * from Scrollable");

			con.createStatement().execute("Insert Into Scrollable(v) Values('qwert')");
			assertRowCount( 1, "Select * from Scrollable");
			assertRowCount( 0, "Select * from Scrollable Where 1=0");

			Statement st = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            testLastWithWhereAssert( st.executeQuery("Select * from Scrollable Where 1=0") );
            testLastWithWhereAssert( st.executeQuery("Select * from Scrollable Where 1=0 Order By v") );
            testLastWithWhereAssert( st.executeQuery("Select v from Scrollable Where 1=0 Order By v") );
            testLastWithWhereAssert( st.executeQuery("Select v from Scrollable Where 1=0 Group By v Order By v") );
		}finally{
            dropTable( con, "Scrollable");
		}
	}
    
    
    private void testLastWithWhereAssert(ResultSet rs) throws Exception{
        assertFalse( "There should be no rows:", rs.last());
        assertFalse( "isLast", rs.isLast());
        try{
            rs.getString("v");
            fail("SQLException 'No current row' should be throw");
        }catch(SQLException ex){
            assertSQLException( "01000", 0, ex );
        }
    }
	

	public void testNextWithWhere() throws Exception{
		Connection con = AllTests.getConnection();
		try{			
			con.createStatement().execute("Create Table Scrollable (i counter, v varchar(20))");
			assertRowCount( 0, "Select * from Scrollable");

			con.createStatement().execute("Insert Into Scrollable(v) Values('qwert')");
			assertRowCount( 1, "Select * from Scrollable");
			assertRowCount( 0, "Select * from Scrollable Where 1=0");

            Statement st = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            testNextWithWhereAssert( st.executeQuery("Select * from Scrollable Where 1=0") );
            testNextWithWhereAssert( st.executeQuery("Select * from Scrollable Where 1=0 Order By v") );
            testNextWithWhereAssert( st.executeQuery("Select v from Scrollable Where 1=0 Group By v") );
            testNextWithWhereAssert( st.executeQuery("Select v from Scrollable Where 1=0 Group By v Order By v") );
		}finally{
            dropTable( con, "Scrollable");
		}
	}
    
    
    private void testNextWithWhereAssert(ResultSet rs) throws Exception{
        assertFalse("There should be no rows:", rs.next());
        try{
            rs.getString("v");
            fail("SQLException 'No current row' should be throw");
        }catch(SQLException ex){
            assertSQLException( "01000", 0, ex);
        }
    }
	
	
	public void testFirstWithWhere() throws Exception{
		Connection con = AllTests.getConnection();
		try{			
			con.createStatement().execute("Create Table Scrollable (i counter, v varchar(20))");
			assertRowCount( 0, "Select * from Scrollable");

			con.createStatement().execute("Insert Into Scrollable(v) Values('qwert')");
			assertRowCount( 1, "Select * from Scrollable");
			assertRowCount( 0, "Select * from Scrollable Where 1=0");

            Statement st = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            testFirstWithWhereAssert( st.executeQuery("Select * from Scrollable Where 1=0") );
            testFirstWithWhereAssert( st.executeQuery("Select * from Scrollable Where 1=0 Order By v") );
            testFirstWithWhereAssert( st.executeQuery("Select v from Scrollable Where 1=0 Group By v") );
            testFirstWithWhereAssert( st.executeQuery("Select v from Scrollable Where 1=0 Group By v Order By v") );
		}finally{
            dropTable( con, "Scrollable");
		}
	}
    
    
    private void testFirstWithWhereAssert(ResultSet rs) throws Exception{
        assertFalse( "isFirst", rs.isFirst() );
        assertTrue( rs.isBeforeFirst() );
        assertFalse( "There should be no rows:", rs.first());
        assertFalse( "isFirst", rs.isFirst() );
        assertTrue( rs.isBeforeFirst() );
        try{
            rs.getString("v");
            fail("SQLException 'No current row' should be throw");
        }catch(SQLException ex){
            assertSQLException("01000", 0, ex);
        }
    }


	public void testPreviousWithWhere() throws Exception{
		Connection con = AllTests.getConnection();
		try{			
			con.createStatement().execute("Create Table Scrollable (i counter, v varchar(20))");
			assertRowCount( 0, "Select * from Scrollable");

			con.createStatement().execute("Insert Into Scrollable(v) Values('qwert')");
			assertRowCount( 1, "Select * from Scrollable");
			assertRowCount( 0, "Select * from Scrollable Where 1=0");

            Statement st = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            testPreviousWithWhereAssert( st.executeQuery("Select * from Scrollable Where 1=0") );
            testPreviousWithWhereAssert( st.executeQuery("Select * from Scrollable Where 1=0 Order By v") );
            testPreviousWithWhereAssert( st.executeQuery("Select v from Scrollable Where 1=0 Group By v") );
            testPreviousWithWhereAssert( st.executeQuery("Select v from Scrollable Where 1=0 Group By v Order By v") );
            testPreviousWithWhereAssert( st.executeQuery("Select v from Scrollable Group By v Having 1=0 Order By v") );
			
		}finally{
            dropTable( con, "Scrollable");
		}
	}
    
    
    private void testPreviousWithWhereAssert(ResultSet rs) throws Exception{
        assertTrue( rs.isBeforeFirst() );
        assertTrue( rs.isAfterLast() );
        rs.afterLast();
        assertTrue( rs.isAfterLast() );
        assertFalse("There should be no rows:", rs.previous());
        try{
            rs.getString("v");
            fail("SQLException 'No current row' should be throw");
        }catch(SQLException ex){
            assertSQLException("01000", 0, ex);
        }
    }


	public void testAbsoluteRelative() throws Exception{
		Connection con = AllTests.getConnection();
		try{			
			con.createStatement().execute("Create Table Scrollable (i counter, v varchar(20))");
			assertRowCount( 0, "Select * from Scrollable");

			con.createStatement().execute("Insert Into Scrollable(v) Values('qwert1')");
			con.createStatement().execute("Insert Into Scrollable(v) Values('qwert2')");
			con.createStatement().execute("Insert Into Scrollable(v) Values('qwert3')");

            Statement st = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            testAbsoluteRelativeAssert( st.executeQuery("Select * from Scrollable") );
            testAbsoluteRelativeAssert( st.executeQuery("Select * from Scrollable Order By i") );
            testAbsoluteRelativeAssert( st.executeQuery("Select v from Scrollable Group By v") );
            testAbsoluteRelativeAssert( st.executeQuery("Select v from Scrollable Group By v Order By v") );
		}finally{
            dropTable( con, "Scrollable");
		}
	}

    private void testAbsoluteRelativeAssert(ResultSet rs) throws SQLException{
        assertEquals(0, rs.getRow());
        
        assertTrue(rs.absolute(2));
        assertEquals("qwert2", rs.getString("v"));
        assertEquals(2, rs.getRow());

        assertTrue(rs.relative(-1));
        assertEquals("qwert1", rs.getString("v"));
        assertEquals(1, rs.getRow());

        assertTrue(rs.absolute(1));
        assertEquals("qwert1", rs.getString("v"));
        assertEquals(1, rs.getRow());
        assertTrue(rs.isFirst());

        assertTrue(rs.relative(1));
        assertEquals("qwert2", rs.getString("v"));
        assertEquals(2, rs.getRow());
        assertFalse(rs.isLast());
        assertFalse(rs.isFirst());

        assertTrue(rs.absolute(-1));
        assertEquals("qwert3", rs.getString("v"));
        assertEquals(3, rs.getRow());
        assertTrue(rs.isLast());
        assertFalse(rs.isFirst());

        assertTrue(rs.relative(0));
        assertEquals("qwert3", rs.getString("v"));
        assertEquals(3, rs.getRow());
        assertTrue(rs.isLast());
        assertFalse(rs.isFirst());

        assertFalse(rs.absolute(4));
        assertEquals(0, rs.getRow());
        assertFalse(rs.isLast());
        assertFalse(rs.isFirst());
        assertFalse(rs.isBeforeFirst());
        assertTrue(rs.isAfterLast());

        assertTrue(rs.last());
        assertEquals(3, rs.getRow());
        assertTrue(rs.isLast());
        assertFalse(rs.isFirst());

        assertFalse(rs.absolute(-4));
        assertEquals(0, rs.getRow());
        assertFalse(rs.isLast());
        assertFalse(rs.isFirst());
        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());

        assertFalse(rs.relative(4));
        assertEquals(0, rs.getRow());
        assertFalse(rs.isLast());
        assertFalse(rs.isFirst());
        assertFalse(rs.isBeforeFirst());
        assertTrue(rs.isAfterLast());

        assertFalse(rs.relative(-4));
        assertEquals(0, rs.getRow());
        assertFalse(rs.isLast());
        assertFalse(rs.isFirst());
        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());
    }

    
    public void testUpdatable() throws Exception{
        Connection con = AllTests.getConnection();
        try{            
            con.createStatement().execute("Create Table Scrollable (i int Identity primary key, v varchar(20))");
            Statement st = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

            for(int row = 1; row < 4; row++){
                testUpdatableAssert( con, st.executeQuery("Select * from Scrollable"), row );
                testUpdatableAssert( con, st.executeQuery("Select * from Scrollable Order By i"), row );
                testUpdatableAssert( con, st.executeQuery("Select * from Scrollable Where 1 = 1"), row );
                testUpdatableAssert( con, st.executeQuery("Select * from Scrollable Where 1 = 1 Order By i"), row );
                con.createStatement().execute("Insert Into Scrollable(v) Values('qwert" +row + "')");
            }
        }finally{
            dropTable( con, "Scrollable");
        }
    }

    private void testUpdatableAssert( Connection con, ResultSet rs, int row) throws Exception{
        con.setAutoCommit(false);
        for(int r=row; r < 4; r++){
            rs.moveToInsertRow();
            rs.updateString( "v", "qwert" + r);
            rs.insertRow();
        }
        
        assertTrue( rs.last() );
        assertEquals( 3, rs.getRow() );     
        
        rs.beforeFirst();
        assertRowCount( 3, rs );
        
        rs.beforeFirst();
        testAbsoluteRelativeAssert(rs);
        
        con.rollback();
        
        assertRowCount( row - 1, con.createStatement().executeQuery("Select * from Scrollable"));
        
        rs.last();
        assertTrue( rs.rowDeleted() );
        assertTrue( rs.rowInserted() );
        rs.beforeFirst();
        assertRowCount( 3, rs );
        
        con.setAutoCommit(true);
    }
    
}
