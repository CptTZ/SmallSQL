/*
 * Created on 14.11.2006
 */
package smallsql.junit;

import java.sql.*;


/**
 * @author Volker Berlin
 */
public class TestAlterTable extends BasicTestCase {

    private final String table = "AlterTable";
    private final int rowCount = 10;
    
    public void setUp(){
        tearDown();
        try{
            Connection con = AllTests.getConnection();
            Statement st = con.createStatement();
            st.execute("create table " + table + "(i int, v varchar(100))");
            st.execute("Insert into " + table + " Values(1,'abc')");
            st.execute("Insert into " + table + " Values(2,'bcd')");
            st.execute("Insert into " + table + " Values(3,'cde')");
            st.execute("Insert into " + table + " Values(4,'def')");
            st.execute("Insert into " + table + " Values(5,'efg')");
            st.execute("Insert into " + table + " Values(6,'fgh')");
            st.execute("Insert into " + table + " Values(7,'ghi')");
            st.execute("Insert into " + table + " Values(8,'hij')");
            st.execute("Insert into " + table + " Values(9,'ijk')");
            st.execute("Insert into " + table + " Values(10,'jkl')");
            st.close();
        }catch(Throwable e){
            e.printStackTrace();
        }
    }
    
    public void tearDown(){
        try {
            dropTable( AllTests.getConnection(), table );
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }
    

    public void testAdd1Column() throws Exception{
        Connection con = AllTests.getConnection();
        Statement st = con.createStatement();
        st.execute("Alter Table " + table + " Add a Varchar(20)");
        ResultSet rs = st.executeQuery("Select * From " + table);
        assertRSMetaData( rs, new String[]{"i", "v", "a"},  new int[]{Types.INTEGER, Types.VARCHAR, Types.VARCHAR} );
    }
    
    
    public void testAdd2Column() throws Exception{
        Connection con = AllTests.getConnection();
        Statement st = con.createStatement();
        st.execute("Alter Table " + table + " Add a Varchar(20), b int DEFAULT 25");
        ResultSet rs = st.executeQuery("Select * From " + table);
        assertRSMetaData( rs, new String[]{"i", "v", "a", "b"},  new int[]{Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.INTEGER} );
        int count = 0;
        while(rs.next()){
            assertEquals( "default value", 25, rs.getInt("b") );
            count++;
        }
        assertEquals( "RowCount", rowCount, count );
    }

    
    public void testAddWithTableLock_REPEATABLE_READ() throws Exception{
        Connection con = AllTests.getConnection();
        Statement st = con.createStatement();
        int isolation = con.getTransactionIsolation();
        con.setAutoCommit(false);
        try{
            con.setTransactionIsolation( Connection.TRANSACTION_REPEATABLE_READ );
            ResultSet rs = st.executeQuery("Select * From " + table);
            rs.next();
            try {
                st.execute("Alter Table " + table + " Add a Varchar(20)");
                fail("Alter Table should not work on a table with a lock.");
            } catch (SQLException ex) {
                assertSQLException( "01000", 0, ex );
            }
            rs.next();
        }finally{
            con.setTransactionIsolation(isolation);
            con.setAutoCommit(true);
        }
    }
    
    
    public void testAddWithTableLock_READ_COMMITTED() throws Exception{
        Connection con = AllTests.getConnection();
        Statement st = con.createStatement();
        int isolation = con.getTransactionIsolation();
        con.setAutoCommit(false);
        try{
            con.setTransactionIsolation( Connection.TRANSACTION_READ_COMMITTED );
            ResultSet rs = st.executeQuery("Select * From " + table);
            rs.next();
            st.execute("Alter Table " + table + " Add a Varchar(20)");
            try {
                rs.next();
                fail("Alter Table should not work on a table with a lock.");
            } catch (SQLException ex) {
                assertSQLException( "01000", 0, ex );
            }
        }finally{
            con.setTransactionIsolation(isolation);
            con.setAutoCommit(true);
        }
    }
    
}
