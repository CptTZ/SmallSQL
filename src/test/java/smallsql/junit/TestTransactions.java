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
 * TestTransactions.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 05.08.2004
 */
package smallsql.junit;
import java.sql.*;

/**
 * @author Volker Berlin
 */
public class TestTransactions extends BasicTestCase {

	
	public void testCreateTable() throws Exception{
		Connection con = AllTests.getConnection();
        Connection con2 = AllTests.createConnection();
		try{			
			con.setAutoCommit(false);
			con.createStatement().execute("create table transactions (ID  INTEGER NOT NULL, Name VARCHAR(100), FirstName VARCHAR(100), Points INTEGER, LicenseID INTEGER, PRIMARY KEY(ID))");
			con.commit();

			con2.setAutoCommit(false);
			
			
			PreparedStatement pr = con2.prepareStatement("insert into transactions (id,Name,FirstName,Points,LicenseID) values (?,?,?,?,?)");
			pr.setInt( 		1, 0 );
			pr.setString( 	2, "Pilot_1" );
			pr.setString( 	3, "Herkules" );
			pr.setInt( 		4, 1 );
			pr.setInt( 		5, 1 );
			pr.addBatch();
			pr.executeBatch();

			assertRowCount( 0, "Select * from transactions");
			con2.commit();
			assertRowCount( 1, "Select * from transactions");
			
		}finally{
            con2.close();
			try{
				con.createStatement().execute("Drop Table transactions");
			}catch(Throwable e){e.printStackTrace();}
			con.setAutoCommit(true);
		}
	}
	


	
	public void testCommit() throws Exception{
		Connection con = AllTests.getConnection();
		try{			
			con.setAutoCommit(false);
			con.createStatement().execute("Create Table transactions (i int identity, v varchar(20))");
			assertRowCount( 0, "Select * from transactions");

			con.createStatement().execute("Insert Into transactions(v) Values('qwert')");
			assertRowCount( 1, "Select * from transactions");

			con.createStatement().execute("Insert Into transactions(v) Select v From transactions");
			assertRowCount( 2, "Select * from transactions");

			con.createStatement().execute("Insert Into transactions Select * From transactions");
			assertRowCount( 4, "Select * from transactions");
			
			con.commit();
			assertRowCount( 4, "Select * from transactions");
			
		}finally{
			try{
				con.createStatement().execute("Drop Table transactions");
			}catch(Throwable e){e.printStackTrace();}
			con.setAutoCommit(true);
		}
	}
	

	/**
	 * In the table there is already one row that is committed.
	 */
	public void testCommitWithOneCommitRow() throws Exception{
		Connection con = AllTests.getConnection();
		try{			
			con.createStatement().execute("Create Table transactions (i int identity, v varchar(20))");
			assertRowCount( 0, "Select * from transactions");

			con.createStatement().execute("Insert Into transactions(v) Values('qwert')");
			assertRowCount( 1, "Select * from transactions");

			con.setAutoCommit(false);
			con.createStatement().execute("Insert Into transactions(v) Select v From transactions");
			assertRowCount( 2, "Select * from transactions");

			con.createStatement().execute("Insert Into transactions (Select * From transactions)");
			assertRowCount( 4, "Select * from transactions");
			
			con.commit();
			assertRowCount( 4, "Select * from transactions");
			
		}finally{
			try{
				con.createStatement().execute("Drop Table transactions");
			}catch(Throwable e){e.printStackTrace();}
			con.setAutoCommit(true);
		}
	}
	

	public void testRollback() throws Exception{
		Connection con = AllTests.getConnection();
		try{			
		    con.createStatement().execute("Create Table transactions (i int identity, v varchar(20))");
			con.setAutoCommit(false);
			assertRowCount( 0, "Select * from transactions");

			con.createStatement().execute("Insert Into transactions(v) Values('qwert')");
			assertRowCount( 1, "Select * from transactions");

			con.createStatement().execute("Insert Into transactions(v) Select v From transactions");
			assertRowCount( 2, "Select * from transactions");

			con.createStatement().execute("Insert Into transactions(v) (Select v From transactions)");
			assertRowCount( 4, "Select * from transactions");
			
			con.rollback();
			assertRowCount( 0, "Select * from transactions");
			
		}finally{
			try{
				con.createStatement().execute("Drop Table transactions");
			}catch(Throwable e){e.printStackTrace();}
			con.setAutoCommit(true);
		}
	}


	/**
	 * In the table there is already one row that is commited.
	 */
	public void testRollbackWithOneCommitRow() throws Exception{
		Connection con = AllTests.getConnection();
		try{			
			con.createStatement().execute("Create Table transactions (i int identity, v varchar(20))");
			assertRowCount( 0, "Select * from transactions");

			con.createStatement().execute("Insert Into transactions(v) Values('qwert')");
			assertRowCount( 1, "Select * from transactions");

			con.setAutoCommit(false);
			con.createStatement().execute("Insert Into transactions(v) Select v From transactions");
			assertRowCount( 2, "Select * from transactions");

			con.createStatement().execute("Insert Into transactions(v) (Select v From transactions)");
			assertRowCount( 4, "Select * from transactions");
			
			con.rollback();
			assertRowCount( 1, "Select * from transactions");
			
		}finally{
			try{
				con.createStatement().execute("Drop Table transactions");
			}catch(Throwable e){e.printStackTrace();}
			con.setAutoCommit(true);
		}
	}


	private void testInsertRow_Last(Connection con, boolean callLastBefore) throws Exception{
		try{			
			con.createStatement().execute("Create Table transactions (i int identity, v varchar(20))");
			assertRowCount( 0, "Select * from transactions");

			con.createStatement().execute("Insert Into transactions(v) Values('qwert')");

			ResultSet rs = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)
								.executeQuery("Select * from transactions Where 1=0");
			
			if(callLastBefore) rs.last();
			rs.moveToInsertRow();
			rs.updateString("v", "qwert2");
			rs.insertRow();
			
			rs.last();
			assertEquals("qwert2", rs.getString("v"));
			assertFalse( rs.next() );
			assertTrue( rs.previous() );
			assertEquals("qwert2", rs.getString("v"));
			
			rs.beforeFirst();
			assertTrue( rs.next() );
			assertEquals("qwert2", rs.getString("v"));
			assertFalse( rs.next() );

		}finally{
			try{
				con.createStatement().execute("Drop Table transactions");
			}catch(Throwable e){e.printStackTrace();}
		}
	}
	
	
	public void testInsertRow_Last() throws Exception{
		Connection con = AllTests.getConnection();
		testInsertRow_Last(con, false);
		testInsertRow_Last(con, true);
		con.setAutoCommit(false);
		testInsertRow_Last(con, false);
		con.setAutoCommit(true);
		con.setAutoCommit(false);
		testInsertRow_Last(con, true);
		con.setAutoCommit(true);
	}

	
	/**
     * Insert a row and update the inserted row in the same transaction. Then make a partial rollback. Test the correct
     * status after of the table after every change of the database.
     * 
     * @throws Exception
     *             if an error occur
     */
	public void testInsertAndUpdate() throws Exception{
		Connection con = AllTests.getConnection();
		try{			
			con.setAutoCommit(false);
			con.createStatement().execute("Create Table transactions ( v varchar(20))");
			assertRowCount( 0, "Select * from transactions");

			assertEquals( 1, con.createStatement().executeUpdate("Insert Into transactions(v) Values('qwert')") );
			assertEqualsRsValue("qwert", "Select * from transactions");
			assertEqualsRsValue(new Integer(1), "Select count(*) from transactions");
			
			assertEquals( 1, con.createStatement().executeUpdate("Update transactions set v='qwert1'") );
			assertEqualsRsValue("qwert1", "Select * from transactions");
			assertEqualsRsValue(new Integer(1), "Select count(*) from transactions");
			
            assertEquals( 1, con.createStatement().executeUpdate("Update transactions set v='qwert2'") );
            assertEqualsRsValue("qwert2", "Select * from transactions");
            assertEqualsRsValue(new Integer(1), "Select count(*) from transactions");
            
			Savepoint savepoint = con.setSavepoint();
			
			assertEquals( 1, con.createStatement().executeUpdate("Update transactions set v='qwert 3'") );
			assertEqualsRsValue("qwert 3", "Select * from transactions");
			assertEqualsRsValue(new Integer(1), "Select count(*) from transactions");

			con.rollback( savepoint );
			
			con.commit();
			assertEqualsRsValue("qwert2", "Select * from transactions");
			assertEqualsRsValue(new Integer(1), "Select count(*) from transactions");
		}finally{
			try{
				con.createStatement().execute("Drop Table transactions");
			}catch(Throwable e){e.printStackTrace();}
			con.setAutoCommit(true);
		}
	}
	
	
	/**
     * The difference to testInsertAndUpdate() is that the row was not inserted in the same transaction
     */
    public void testUpdateAndSavepoint() throws Exception{
        Connection con = AllTests.getConnection();
        try{
            con.createStatement().execute("Create Table transactions ( v varchar(20))");
            assertRowCount(0, "Select * from transactions");

            assertEquals(1, con.createStatement().executeUpdate("Insert Into transactions(v) Values('qwert')"));
            assertEqualsRsValue("qwert", "Select * from transactions");
            assertEqualsRsValue(new Integer(1), "Select count(*) from transactions");

            // start the transaction after the row is already insert
            con.setAutoCommit(false);
            assertEquals(1, con.createStatement().executeUpdate("Update transactions set v='qwert1'"));
            assertEqualsRsValue("qwert1", "Select * from transactions");
            assertEqualsRsValue(new Integer(1), "Select count(*) from transactions");

            assertEquals(1, con.createStatement().executeUpdate("Update transactions set v='qwert2'"));
            assertEqualsRsValue("qwert2", "Select * from transactions");
            assertEqualsRsValue(new Integer(1), "Select count(*) from transactions");

            Savepoint savepoint = con.setSavepoint();

            assertEquals(1, con.createStatement().executeUpdate("Update transactions set v='qwert 3'"));
            assertEqualsRsValue("qwert 3", "Select * from transactions");
            assertEqualsRsValue(new Integer(1), "Select count(*) from transactions");

            assertEquals(1, con.createStatement().executeUpdate("Update transactions set v='qwert 4'"));
            assertEqualsRsValue("qwert 4", "Select * from transactions");
            assertEqualsRsValue(new Integer(1), "Select count(*) from transactions");

            assertEquals(1, con.createStatement().executeUpdate("Update transactions set v='qwert 5'"));
            assertEqualsRsValue("qwert 5", "Select * from transactions");
            assertEqualsRsValue(new Integer(1), "Select count(*) from transactions");

            con.rollback(savepoint);

            con.commit();
            assertEqualsRsValue("qwert2", "Select * from transactions");
            assertEqualsRsValue(new Integer(1), "Select count(*) from transactions");
        }finally{
            dropTable(con, "transactions");
            con.setAutoCommit(true);
        }
    }
    
    
	/**
     * If there was insert a row within the ResultSet that not map WHERE than you scroll to this row. If there an Insert
     * outsite the ResultSet that not map the WHERE then you can't scroll this row.
     * 
     * @throws Exception
     */
	public void testInsertRow_withWrongWhere() throws Exception{
		Connection con = AllTests.getConnection();
		try{			
			con.setAutoCommit(false);
			con.createStatement().execute("Create Table transactions (i int identity, v varchar(20))");
			assertRowCount( 0, "Select * from transactions");

			con.createStatement().execute("Insert Into transactions(v) Values('qwert')");

			ResultSet rs = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)
								.executeQuery("Select * from transactions Where 1=0");
			
			rs.moveToInsertRow();
			rs.updateString("v", "qwert2");
			rs.insertRow();
			
			rs.beforeFirst();
			assertTrue( rs.next() );
			assertEquals("qwert2", rs.getString("v"));
			assertFalse( rs.next() );
		}finally{
			try{
				con.createStatement().execute("Drop Table transactions");
			}catch(Throwable e){e.printStackTrace();}
			con.setAutoCommit(true);
		}
	}
	
	

	/**
	 * A row that was inserted and committed with a valid WHERE expression should not count 2 times.
	 */
	public void testInsertRow_withRightWhere() throws Exception{
		Connection con = AllTests.getConnection();
		try{			
			con.createStatement().execute("Create Table transactions (i int identity, v varchar(20))");
			assertRowCount( 0, "Select * from transactions");

			con.createStatement().execute("Insert Into transactions(v) Values('qwert2')");

			ResultSet rs = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)
								.executeQuery("Select * from transactions Where v = 'qwert'");
			
			rs.moveToInsertRow();
			rs.updateString("v", "qwert");
			rs.insertRow();
			
			rs.beforeFirst();
			assertTrue( rs.next() );
			assertEquals("qwert", rs.getString("v"));
			assertFalse( rs.next() );
		}finally{
			try{
				con.createStatement().execute("Drop Table transactions");
			}catch(Throwable e){e.printStackTrace();}
		}
	}
	
	
	public void testReadUncommited() throws Exception{
		Connection con1 = AllTests.getConnection();
		Connection con2 = AllTests.createConnection();
		try{		
			con2.setTransactionIsolation( Connection.TRANSACTION_READ_UNCOMMITTED );
			con1.createStatement().execute("Create Table transactions (i int identity, v varchar(20))");
			assertRowCount( 0, "Select * from transactions");

			con1.setAutoCommit(false);
			con1.createStatement().execute("Insert Into transactions(v) Values('qwert2')");

			ResultSet rs2 = con2.createStatement().executeQuery("Select count(*) from transactions");
			assertTrue( rs2.next() );
			assertEquals( 1, rs2.getInt(1) );
		}finally{
		    dropTable(con1, "transactions");
			con1.setAutoCommit(true);
			con2.close();
		}
	}

	
	public void testReadCommited() throws Exception{
		Connection con1 = AllTests.getConnection();
		Connection con2 = AllTests.createConnection();
		try{		
			con2.setTransactionIsolation( Connection.TRANSACTION_READ_COMMITTED );
			con1.createStatement().execute("Create Table transactions (i int identity, v varchar(20))");
			assertRowCount( 0, "Select * from transactions");

			con1.setAutoCommit(false);
			con1.createStatement().execute("Insert Into transactions(v) Values('qwert2')");

			ResultSet rs2 = con2.createStatement().executeQuery("Select count(*) from transactions");
			assertTrue( rs2.next() );
			assertEquals( 0, rs2.getInt(1) );
		}finally{
            dropTable(con1, "transactions");
			con1.setAutoCommit(true);
			con2.close();
		}
	}


    public void testReadSerialized() throws Exception{
        Connection con1 = AllTests.getConnection();
        Connection con2 = AllTests.createConnection();
        try{        
            con1.createStatement().execute("Create Table transactions (i int identity, v varchar(20))");
            assertRowCount( 0, "Select * from transactions");
            con1.createStatement().execute("Insert Into transactions(v) Values('qwert2')");
            assertRowCount( 1, "Select * from transactions");

            con1.setTransactionIsolation( Connection.TRANSACTION_SERIALIZABLE );
            con1.setAutoCommit(false);

            //create a serialize lock on the table
            ResultSet rs1 = con1.createStatement().executeQuery("Select count(*) from transactions");
            assertTrue( rs1.next() );
            assertEquals( "Count(*)", 1, rs1.getInt(1) );
            
            //reading should be possible on a second connection
            ResultSet rs2 = con2.createStatement().executeQuery("Select count(*) from transactions");
            assertTrue( rs2.next() );
            assertEquals( "Count(*)", 1, rs2.getInt(1) );
            try{
                con2.createStatement().execute("Insert Into transactions(v) Values('qwert3')");
                fail("TRANSACTION_SERIALIZABLE does not lock the table");
            }catch(SQLException ex){
                assertSQLException("01000", 0, ex);
            }
        }finally{
            con2.close();
            dropTable(con1, "transactions");
            con1.setAutoCommit(true);
        }
    }


	public void testReadWriteLock() throws Exception{
		Connection con1 = AllTests.getConnection();
		Connection con2 = AllTests.createConnection();
		try{		
			con1.createStatement().execute("Create Table transactions (i int identity, v varchar(20))");
			con1.createStatement().execute("Insert Into transactions(v) Values('qwert1')");

			con1.setAutoCommit(false);
			con1.createStatement().execute("Update transactions Set v = 'qwert'");
			
			long time = System.currentTimeMillis();
			try{
				con2.createStatement().executeQuery("Select count(*) from transactions");
			}catch(SQLException ex){
			    assertSQLException("01000", 0, ex);
			}		
			assertTrue("Wait time to small", System.currentTimeMillis()-time>=5000);
		}finally{
		    con2.close();
			con1.setAutoCommit(true);
			dropTable(con1, "transactions");
		}
	}


}
