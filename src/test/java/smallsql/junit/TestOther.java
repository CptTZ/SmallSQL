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
 * TestOthers.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 31.07.2004
 */
package smallsql.junit;

import java.sql.*;

/**
 * @author Volker Berlin
 */
public class TestOther extends BasicTestCase {

	public void testInsertSelect() throws Exception{
		Connection con = AllTests.getConnection();
		try{
			con.createStatement().execute("Create Table InsertSelect (i counter, v varchar(20))");
			assertEqualsRsValue( new Integer(0), "Select count(*) from InsertSelect");

			con.createStatement().execute("Insert Into InsertSelect(v) Values('qwert')");
			assertEqualsRsValue( new Integer(1), "Select count(*) from InsertSelect");

			con.createStatement().execute("Insert Into InsertSelect(v) Select v From InsertSelect");
			assertEqualsRsValue( new Integer(2), "Select count(*) from InsertSelect");

			con.createStatement().execute("Insert Into InsertSelect(v) (Select v From InsertSelect)");
			assertEqualsRsValue( new Integer(4), "Select count(*) from InsertSelect");
		}finally{
            dropTable( con, "InsertSelect" );
		}
	}


	public void testDistinct() throws Exception{
		Connection con = AllTests.getConnection();
		try{
			con.createStatement().execute("Create Table TestDistinct (i counter, v varchar(20), n bigint, b boolean)");
			assertRowCount( 0, "Select * From TestDistinct" );

			con.createStatement().execute("Insert Into TestDistinct(v,b) Values('qwert1',true)");
			con.createStatement().execute("Insert Into TestDistinct(v,b) Values('qwert2',true)");
			con.createStatement().execute("Insert Into TestDistinct(v,b) Values('qwert1',true)");
			con.createStatement().execute("Insert Into TestDistinct(v,b) Values('qwert2',true)");
			con.createStatement().execute("Insert Into TestDistinct(v,b) Values('qwert1',false)");
			assertRowCount( 5, "Select b,n,v From TestDistinct" );
			assertRowCount( 2, "Select Distinct v From TestDistinct t1" );
			assertRowCount( 3, "Select Distinct b,n,v From TestDistinct" );
			assertRowCount( 3, "Select Distinct b,n,v,i+null,23+i-i,'asdf'+v From TestDistinct" );
			assertRowCount( 5, "Select All b,n,v From TestDistinct" );
		}finally{
            dropTable( con, "TestDistinct" );
		}
	}
	

	public void testConstantAndRowPos() throws Exception{
		assertRowCount( 1, "Select 12, 'qwert'" );
	}
	

	public void testNoFromResult() throws Exception{
		Connection con = AllTests.getConnection();
		
		Statement st = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY );
		ResultSet rs = st.executeQuery("Select 12, 'qwert' alias");
		
		assertRSMetaData( rs, new String[]{"col1", "alias"}, new int[]{Types.INTEGER, Types.VARCHAR });
		
		assertTrue( rs.isBeforeFirst() );
		assertFalse( rs.isFirst() );
		assertFalse( rs.isLast() );
		assertFalse( rs.isAfterLast() );
		
		assertTrue( rs.next() );
		assertFalse( rs.isBeforeFirst() );
		assertTrue( rs.isFirst() );
		assertTrue( rs.isLast() );
		assertFalse( rs.isAfterLast() );
		
		assertFalse( rs.next() );
		assertFalse( rs.isBeforeFirst() );
		assertFalse( rs.isFirst() );
		assertFalse( rs.isLast() );
		assertTrue( rs.isAfterLast() );
		
		assertTrue( rs.previous() );
		assertFalse( rs.isBeforeFirst() );
		assertTrue( rs.isFirst() );
		assertTrue( rs.isLast() );
		assertFalse( rs.isAfterLast() );
		
		assertFalse( rs.previous() );
		assertTrue( rs.isBeforeFirst() );
		assertFalse( rs.isFirst() );
		assertFalse( rs.isLast() );
		assertFalse( rs.isAfterLast() );
		
		assertTrue( rs.first() );
		assertFalse( rs.isBeforeFirst() );
		assertTrue( rs.isFirst() );
		assertTrue( rs.isLast() );
		assertFalse( rs.isAfterLast() );
		
		assertTrue( rs.last() );
		assertFalse( rs.isBeforeFirst() );
		assertTrue( rs.isFirst() );
		assertTrue( rs.isLast() );
		assertFalse( rs.isAfterLast() );
	}

	
	public void testInSelect() throws Exception{
		Connection con = AllTests.getConnection();
		try{
			con.createStatement().execute("Create Table TestInSelect (i counter, v varchar(20), n bigint, b boolean)");
			assertRowCount( 0, "Select * From TestInSelect WHere i In (Select i from TestInSelect)" );

			con.createStatement().execute("Insert Into TestInSelect(v,b) Values('qwert1',true)");
			assertRowCount( 1, "Select * From TestInSelect WHere i In (Select i from TestInSelect)" );

			con.createStatement().execute("Insert Into TestInSelect(v,b) Values('qwert1',true)");
			assertRowCount( 2, "Select * From TestInSelect WHere i In (Select i from TestInSelect)" );
			assertRowCount( 1, "Select * From TestInSelect WHere i In (Select i from TestInSelect Where i>1)" );
			assertRowCount( 1, "Select * From TestInSelect Where i IN ( 1, 1, 12345, 987654321)" );
			assertRowCount( 2, "Select * From TestInSelect Where v IN ( null, '', 'qwert1', 'qwert1')" );
			assertRowCount( 2, "Select * From TestInSelect Where v IN ( 'qwert1')" );
			assertRowCount( 0, "Select * From TestInSelect Where '' IN ( 'qwert1')" );
			assertRowCount( 2, "Select * From TestInSelect Where 'qwert1' IN ( 'qwert1', 'qwert2')" );
		}finally{
            dropTable( con, "TestInSelect" );
		}
	}

	
	public void testSetTransaction() throws Exception{
		Connection con = AllTests.getConnection();
		try{
			con.createStatement().execute("Set Transaction Isolation Level Read Uncommitted");
			assertEquals( Connection.TRANSACTION_READ_UNCOMMITTED, con.getTransactionIsolation() );
			
			con.createStatement().execute("Set Transaction Isolation Level Read Committed");
			assertEquals( Connection.TRANSACTION_READ_COMMITTED, con.getTransactionIsolation() );
			
			con.createStatement().execute("Set Transaction Isolation Level Repeatable Read");
			assertEquals( Connection.TRANSACTION_REPEATABLE_READ, con.getTransactionIsolation() );
			
			con.createStatement().execute("Set Transaction Isolation Level Serializable");
			assertEquals( Connection.TRANSACTION_SERIALIZABLE, con.getTransactionIsolation() );
			
		}finally{
			con.setTransactionIsolation( Connection.TRANSACTION_READ_COMMITTED );
		}
	}
	
	
	public void testCreateDropDatabases() throws Exception{
		Connection con = DriverManager.getConnection("jdbc:smallsql");
		
		Statement st = con.createStatement();
		try{
			st.execute("Create Database anyTestDatabase");
		}catch(SQLException ex){
			st.execute("Drop Database anyTestDatabase");
			throw ex;
		}
		st.execute("Drop Database anyTestDatabase");
	}
	
	
	public void testManyColumns() throws Exception{
		Connection con = AllTests.getConnection();
		Statement st = con.createStatement();
        dropTable( con, "ManyCols" );
		StringBuffer buf = new StringBuffer("Create Table ManyCols(");
		for(int i=1; i<300; i++){
			if(i!=1)buf.append(',');
			buf.append("column").append(i).append(" int");
		}
		buf.append(')');
		
		st.execute(buf.toString());
		con.close();
		con = AllTests.getConnection();
		st = con.createStatement();
		assertEquals(1,st.executeUpdate("Insert Into ManyCols(column260) Values(123456)"));
		st.execute("Drop Table ManyCols");
	}


    /**
     * If a CHAR and a VARCHAR data type in a equals then both data type should be identical.
     * This means the database must convert the CHAR to VARCHAR or vice versa.
     * A static or dynamic string parameter has the data type VARCHAR
     */
    public void testCharEqualsVarchar() throws Exception{
        Connection con = AllTests.getConnection();
        try{
            con.createStatement().execute("Create Table CharEqualsVarchar (c char(10))");
            assertRowCount( 0, "Select * From CharEqualsVarchar" );

            con.createStatement().execute("Insert Into CharEqualsVarchar(c) Values('qwert1')");
            assertRowCount( 1, "Select * From CharEqualsVarchar" );

            assertRowCount( 1, "Select * From CharEqualsVarchar Where c = 'qwert1'" );
            assertRowCount( 0, "Select * From CharEqualsVarchar Where c = 'qwert1        xxxx'" );
            assertRowCount( 1, "Select * From CharEqualsVarchar Where c = cast('qwert1' as char(8))" );
            assertRowCount( 1, "Select * From CharEqualsVarchar Where c = cast('qwert1' as char(12))" );
            assertRowCount( 1, "Select * From CharEqualsVarchar Where c In('qwert1')" );
            assertRowCount( 0, "Select * From CharEqualsVarchar Where c In('qwert1        xxxx')" );
            
            PreparedStatement pr;
            pr = con.prepareStatement( "Select * From CharEqualsVarchar Where c = ?" );
            pr.setString( 1, "qwert1" );
            assertRowCount( 1, pr.executeQuery() );
            pr.setString( 1, "qwert1        xxxx" );
            assertRowCount( 0, pr.executeQuery() );
        }finally{
            dropTable( con, "CharEqualsVarchar" );
        }
    }

    
    public void testLike() throws Exception{
        Connection con = AllTests.getConnection();
        try{
            con.createStatement().execute("Create Table Like (c varchar(20))");

            con.createStatement().execute("Insert Into Like(c) Values('qwert1')");
            con.createStatement().execute("Insert Into Like(c) Values('qwert2')");
            con.createStatement().execute("Insert Into Like(c) Values('qwert2.5')");
            con.createStatement().execute("Insert Into Like(c) Values('awert1')");
            con.createStatement().execute("Insert Into Like(c) Values('awert2')");
            con.createStatement().execute("Insert Into Like(c) Values('awert3')");
            con.createStatement().execute("Insert Into Like(c) Values('qweSGSGSrt1')");
            
            assertRowCount( 2, "Select * From Like Where c like 'qwert_'" );
            assertRowCount( 3, "Select * From Like Where c like 'qwert%'" );
            assertRowCount( 2, "Select * From Like Where c like 'qwert2%'" );

            assertRowCount( 6, "Select * From Like Where c like '_wert%'" );
            assertRowCount( 2, "Select * From Like Where c like 'qwe%rt1'" );
            assertRowCount( 3, "Select * From Like Where c like 'qwe%rt_'" );
            assertRowCount( 7, "Select * From Like Where c like '%_'" );

        }finally{
            dropTable( con, "Like" );
        }
    }
    
    
    public void testBinaryStore() throws Exception{
        Connection con = AllTests.getConnection();
        try{
            Statement st = con.createStatement();
            st.execute("Create Table Binary (b varbinary(20))");
            
            st.execute("Truncate Table Binary");
            st.execute("Insert Into Binary(b) Values(12345)");
            ResultSet rs = st.executeQuery("Select * From Binary");
            rs.next();
            assertEquals(rs.getInt(1), 12345);
            
            st.execute("Truncate Table Binary");
            st.execute("Insert Into Binary(b) Values(1.2345)");
            rs = st.executeQuery("Select * From Binary");
            rs.next();
            assertEquals( 1.2345, rs.getDouble(1), 0.0);
            
            st.execute("Truncate Table Binary");
            st.execute("Insert Into Binary(b) Values(cast(1.2345 as real))");
            rs = st.executeQuery("Select * From Binary");
            rs.next();
            assertEquals( 1.2345F, rs.getFloat(1), 0.0);
            
        }finally{
            dropTable( con, "Binary" );
        }
    }
    
    
    public void testCatalog() throws Exception{
        Connection con = DriverManager.getConnection("jdbc:smallsql");
        assertEquals( "", con.getCatalog() );
        con.setCatalog( AllTests.CATALOG );
        assertEquals( AllTests.CATALOG, con.getCatalog() ); 
        con.close();
        
        con = DriverManager.getConnection("jdbc:smallsql");
        assertEquals( "", con.getCatalog() );
        con.createStatement().execute( "Use " + AllTests.CATALOG );
        assertEquals( AllTests.CATALOG, con.getCatalog() ); 
        con.close();
        
        con = DriverManager.getConnection("jdbc:smallsql?dbpath=" + AllTests.CATALOG);
        assertEquals( AllTests.CATALOG, con.getCatalog() ); 
        con.close();
    }
}
