/*
 * Created on 14.11.2006
 */
package smallsql.junit;

import java.sql.*;


/**
 * @author Volker Berlin
 */
public class TestAlterTable2 extends BasicTestCase {

    private final String table = "AlterTable2";
    
    public void setUp(){
        tearDown();
    }
    
    public void tearDown(){
        try {
            dropTable( AllTests.getConnection(), table );
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }
    

    public void testWithPrimaryKey() throws Exception{
        Connection con = AllTests.getConnection();
        Statement st = con.createStatement();
        st.execute("create table "+table+" (keyField varchar(2) primary key)");
        st.execute("alter table "+table+" add anotherField varchar(4)");
        ResultSet rs = st.executeQuery("Select * From " + table);
        assertRSMetaData( rs, new String[]{"keyField", "anotherField"},  new int[]{Types.VARCHAR, Types.VARCHAR} );
        rs = con.getMetaData().getIndexInfo( null, null, table, false, false );
        assertRowCount( 1, rs );
    }
    
    
    public void testAddPrimaryKey() throws Exception{
        Connection con = AllTests.getConnection();
        Statement st = con.createStatement();
        st.execute("create table "+table+" (a varchar(2))");
        st.execute("alter table "+table+" add b varchar(4) primary key");
        ResultSet rs = st.executeQuery("Select * From " + table);
        assertRSMetaData( rs, new String[]{"a", "b"},  new int[]{Types.VARCHAR, Types.VARCHAR} );
        rs = con.getMetaData().getIndexInfo( null, null, table, false, false );
        assertRowCount( 1, rs );
    }
    
    
    public void testAdd2PrimaryKeys() throws Exception{
        Connection con = AllTests.getConnection();
        Statement st = con.createStatement();
        st.execute("create table "+table+" (a varchar(2) primary key)");
        
        try {
            st.execute("alter table "+table+" add b varchar(4) primary key");
            fail("2 primary keys are invalid");
        } catch (SQLException ex) {
            assertSQLException("01000",0, ex);
        }
        ResultSet rs = st.executeQuery("Select * From " + table);
        assertRSMetaData( rs, new String[]{"a"},  new int[]{Types.VARCHAR} );
        rs = con.getMetaData().getIndexInfo( null, null, table, false, false );
        assertRowCount( 1, rs );
    }
    

    public void testAdd2Keys() throws Exception{
        Connection con = AllTests.getConnection();
        Statement st = con.createStatement();
        st.execute("create table "+table+" (a varchar(2) unique)");
        st.execute("alter table "+table+" add b varchar(4) primary key");
        ResultSet rs = st.executeQuery("Select * From " + table);
        assertRSMetaData( rs, new String[]{"a", "b"},  new int[]{Types.VARCHAR, Types.VARCHAR} );
        rs = con.getMetaData().getIndexInfo( null, null, table, false, false );
        assertRowCount( 2, rs );
    }

}
