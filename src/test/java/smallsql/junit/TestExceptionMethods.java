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
 * TestExceptionMethods.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 02.03.2005
 */
package smallsql.junit;

import java.io.File;
import java.sql.*;

/**
 * @author Volker Berlin
 */
public class TestExceptionMethods extends BasicTestCase{

    public void testForwardOnly() throws Exception{
        Connection con = AllTests.getConnection();
        try{
            con.createStatement().execute("Create Table ExceptionMethods(v varchar(30))");

            con.createStatement().execute("Insert Into ExceptionMethods(v) Values('qwert')");

            ResultSet rs = con.createStatement().executeQuery("Select * from ExceptionMethods");
            assertEquals(true, rs.next());

            try{
                rs.isBeforeFirst();
                fail("SQLException 'ResultSet is forward only' should be throw");
            }catch(SQLException e){
                assertSQLException("01000", 0, e);
            }

            try{
                rs.isFirst();
                fail("SQLException 'ResultSet is forward only' should be throw");
            }catch(SQLException e){
                assertSQLException("01000", 0, e);
            }

            try{
                rs.first();
                fail("SQLException 'ResultSet is forward only' should be throw");
            }catch(SQLException e){
                assertSQLException("01000", 0, e);
            }

            try{
                rs.previous();
                fail("SQLException 'ResultSet is forward only' should be throw");
            }catch(SQLException e){
                assertSQLException("01000", 0, e);
            }

            try{
                rs.last();
                fail("SQLException 'ResultSet is forward only' should be throw");
            }catch(SQLException e){
                assertSQLException("01000", 0, e);
            }

            try{
                rs.isLast();
                fail("SQLException 'ResultSet is forward only' should be throw");
            }catch(SQLException e){
                assertSQLException("01000", 0, e);
            }

            try{
                rs.isAfterLast();
                fail("SQLException 'ResultSet is forward only' should be throw");
            }catch(SQLException e){
                assertSQLException("01000", 0, e);
            }

            try{
                rs.afterLast();
                fail("SQLException 'ResultSet is forward only' should be throw");
            }catch(SQLException e){
                assertSQLException("01000", 0, e);
            }

            try{
                rs.absolute(1);
                fail("SQLException 'ResultSet is forward only' should be throw");
            }catch(SQLException e){
                assertSQLException("01000", 0, e);
            }

            try{
                rs.relative(1);
                fail("SQLException 'ResultSet is forward only' should be throw");
            }catch(SQLException e){
                assertSQLException("01000", 0, e);
            }
        }finally{
            dropTable(con, "ExceptionMethods");
        }
    }


    public void testGetConnection() throws Exception{
        Connection con;
        try{
            con = DriverManager.getConnection(AllTests.JDBC_URL + "?abc");
            con.close();
            fail("SQLException should be thrown");
        }catch(SQLException ex){
            // is OK
        }
        con = DriverManager.getConnection(AllTests.JDBC_URL + "? ");
        con.close();

        con = DriverManager.getConnection(AllTests.JDBC_URL + "?a=b; ; c=d  ; e = f; ; ");

        // open 2 Connections with different written path
        Connection con2 = DriverManager.getConnection( "jdbc:smallsql:" + new File( AllTests.CATALOG ).getAbsolutePath());
        con.close();
        con2.close();

        con = DriverManager.getConnection( "jdbc:smallsql:file:" + AllTests.CATALOG );
        con.close();
    }


    public void testDuplicatedColumnCreate() throws Exception{
        Connection con = AllTests.getConnection();
        Statement st = con.createStatement();
        try{
            st.execute("Create Table DuplicatedColumn(col INT, Col INT)");
            fail("SQLException 'Duplicated Column' should be throw");
        }catch(SQLException e){
            assertSQLException("01000", 0, e);
        }
    }


    public void testDuplicatedColumnAlter() throws Exception{
        Connection con = AllTests.getConnection();
        try{
            Statement st = con.createStatement();
            st.execute("Create Table DuplicatedColumn(col INT)");
            try{
                st.execute("ALTER TABLE DuplicatedColumn Add Col INT");
                fail("SQLException 'Duplicated Column' should be throw");
            }catch(SQLException e){
                assertSQLException("01000", 0, e);
            }
        }finally{
            dropTable(con, "DuplicatedColumn");
        }
    }


    public void testDuplicatedColumnInsert() throws Exception{
        Connection con = AllTests.getConnection();
        try{
            Statement st = con.createStatement();
            st.execute("Create Table DuplicatedColumn(col INT)");
            try{
                st.execute("INSERT INTO DuplicatedColumn(col,Col) Values(1,2)");
                fail("SQLException 'Duplicated Column' should be throw");
            }catch(SQLException e){
                assertSQLException("01000", 0, e);
            }
        }finally{
            dropTable(con, "DuplicatedColumn");
        }
    }


    /**
     * The fail of creating table should not produce any files 
     */
    public void testDuplicatedCreateTable() throws Exception{
        Connection con = AllTests.getConnection();
        try{
            dropTable(con, "DuplicatedTable");
            Statement st = con.createStatement();
            st.execute("Create Table DuplicatedTable(col INT primary key)");
            int tableFileCount = countFiles("DuplicatedTable");
            try{
                st.execute("Create Table DuplicatedTable(col INT primary key)");
                fail("SQLException 'Duplicated Table' should be throw");
            }catch(SQLException e){
                assertSQLException("01000", 0, e);
            }
            assertEquals("Additional Files created",tableFileCount, countFiles("DuplicatedTable"));
        }finally{
            dropTable(con, "DuplicatedTable");
        }
    }
    
    
    private int countFiles(String fileNameStart){
        int count = 0;
        String names[] = new File(AllTests.CATALOG).list();
        for(int i=0; i<names.length; i++){
            if(names[i].startsWith(fileNameStart)){
                count++;
            }
        }
        return count;
    }


    public void testAmbiguousColumn() throws Exception{
        Connection con = AllTests.getConnection();
        try{
            Statement st = con.createStatement();
            st.execute("create table foo (myint number)");
            st.execute("create table bar (myint number)");
            try{
                st.executeQuery("select myint from foo, bar");
                fail("SQLException 'Ambiguous name' should be throw");
            }catch(SQLException e){
                assertSQLException("01000", 0, e);
            }
        }finally{
            dropTable(con, "foo");
            dropTable(con, "bar");
        }
    }


    public void testClosedStatement() throws Exception{
        Connection con = AllTests.getConnection();
        Statement st = con.createStatement();
        st.close();
        try{
            st.execute("Select 1");
            fail("Exception should throw");
        }catch(SQLException ex){
            assertSQLException("HY010", 0, ex);
        }
        try{
            st.executeQuery("Select 1");
            fail("Exception should throw");
        }catch(SQLException ex){
            assertSQLException("HY010", 0, ex);
        }
        try{
            st.executeUpdate("Select 1");
            fail("Exception should throw");
        }catch(SQLException ex){
            assertSQLException("HY010", 0, ex);
        }
    }


    public void testClosedPreparedStatement() throws Exception{
        Connection con = AllTests.getConnection();
        PreparedStatement pr = con.prepareStatement("Select ?");
        pr.setInt(1, 1);
        pr.close();
        try{
            pr.setInt(1, 1);
            fail("Exception should throw");
        }catch(SQLException ex){
            assertSQLException("HY010", 0, ex);
        }
        try{
            pr.execute();
            fail("Exception should throw");
        }catch(SQLException ex){
            assertSQLException("HY010", 0, ex);
        }
        try{
            pr.executeQuery();
            fail("Exception should throw");
        }catch(SQLException ex){
            assertSQLException("HY010", 0, ex);
        }
        try{
            pr.executeUpdate();
            fail("Exception should throw");
        }catch(SQLException ex){
            assertSQLException("HY010", 0, ex);
        }
    }

}
