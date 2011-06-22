/*
 * Created on 13.07.2008
 */
package smallsql.junit;

import java.sql.*;
import java.util.ArrayList;

/**
 * Test some thread problems.
 * 
 * @author Volker Berlin
 */
public class TestThreads extends BasicTestCase{

    volatile Throwable throwable;


    /**
     * Test the concurrently read of a table
     * 
     * @throws Throwable
     *             if an thread problem occur
     */
    public void testConcurrentRead() throws Throwable{
        ArrayList threadList = new ArrayList();
        throwable = null;

        // Any table from another test that include rows.
        final String sql = "Select * From table_OrderBy1";

        // calculate the row count of this table
        final Connection con = AllTests.getConnection();
        Statement st = con.createStatement();
        ResultSet rs = st.executeQuery("Select * From table_OrderBy1");
        int count = 0;
        while(rs.next()){
            count++;
        }
        final int rowCount = count;

        // start threads that check the row count
        for(int i = 0; i < 200; i++){
            Thread thread = new Thread(new Runnable(){

                public void run(){
                    try{
                        assertRowCount(rowCount, sql);
                    }catch(Throwable ex){
                        throwable = ex;
                    }
                }

            });
            threadList.add(thread);
            thread.start();
        }

        // wait until all threads are finish
        for(int i = 0; i < threadList.size(); i++){
            Thread thread = (Thread)threadList.get(i);
            thread.join(5000);
        }

        // throw the exception if one occur
        if(throwable != null){
            throw throwable;
        }
    }


    /**
     * Create a table with a single row. In different threads on the same connection a int value will be increment.
     * 
     * @throws Throwable
     *             if an thread problem occur
     */
    public void testConcurrentThreadWrite() throws Throwable{
        ArrayList threadList = new ArrayList();
        throwable = null;
        final Connection con = AllTests.getConnection();
        Statement st = con.createStatement();
        try{
            st.execute("CREATE TABLE ConcurrentWrite( value int)");
            st.execute("INSERT INTO ConcurrentWrite(value) Values(0)");

            // start threads that check the row count
            for(int i = 0; i < 200; i++){
                Thread thread = new Thread(new Runnable(){

                    public void run(){
                        try{
                            Statement st2 = con.createStatement();
                            int count = st2.executeUpdate("UPDATE ConcurrentWrite SET value = value + 1");
                            assertEquals("Update Count", 1, count);
                        }catch(Throwable ex){
                            throwable = ex;
                        }
                    }

                });
                threadList.add(thread);
                thread.start();
            }

            // wait until all threads are finish
            for(int i = 0; i < threadList.size(); i++){
                Thread thread = (Thread)threadList.get(i);
                thread.join(5000);
            }

            // throw the exception if one occur
            if(throwable != null){
                throw throwable;
            }

            assertEqualsRsValue(new Integer(200), "SELECT value FROM ConcurrentWrite");
        }finally{
            dropTable(con, "ConcurrentWrite");
        }
    }


    /**
     * Create a table with a single row. In different connections a int value will be increment.
     * 
     * @throws Throwable
     *             if an thread problem occur
     */
    public void testConcurrentConnectionWrite() throws Throwable{
        ArrayList threadList = new ArrayList();
        throwable = null;
        Connection con = AllTests.getConnection();
        Statement st = con.createStatement();
        try{
            st.execute("CREATE TABLE ConcurrentWrite( value int)");
            st.execute("INSERT INTO ConcurrentWrite(value) Values(0)");

            // start threads that check the row count
            for(int i = 0; i < 200; i++){
                Thread thread = new Thread(new Runnable(){

                    public void run(){
                        try{
                            Connection con2 = AllTests.createConnection();
                            Statement st2 = con2.createStatement();
                            int count = st2.executeUpdate("UPDATE ConcurrentWrite SET value = value + 1");
                            assertEquals("Update Count", 1, count);
                            con2.close();
                        }catch(Throwable ex){
                            throwable = ex;
                        }
                    }

                });
                threadList.add(thread);
                thread.start();
            }

            // wait until all threads are finish
            for(int i = 0; i < threadList.size(); i++){
                Thread thread = (Thread)threadList.get(i);
                thread.join(5000);
            }

            // throw the exception if one occur
            if(throwable != null){
                throw throwable;
            }

            assertEqualsRsValue(new Integer(200), "SELECT value FROM ConcurrentWrite");
        }finally{
            dropTable(con, "ConcurrentWrite");
        }
    }

}
