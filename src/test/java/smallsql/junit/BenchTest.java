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
 * BenchTest.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.junit;

import java.sql.*;

public class BenchTest
{
    static byte[] byteArray = {23, 34, 67 };
    static byte[] largeByteArray = new byte[4000];
    
    static String driverClassName = "smallsql.database.SSDriver";
    static String userName        = "sa";
    static String password        = "";
    static String jdbcUrl         = "jdbc:smallsql:AllTests";
    static int    rowCount        = 10000;
    
    static Connection con;
    static final String tableName = "BenchTest2";
        
    
    public static void main(String[] args) throws SQLException{
        for(int i=0; i<args.length;){
            String option = args[i++];
            if      (option.equals("-driver")  ) driverClassName = args[i++];
            else if (option.equals("-user")    ) userName = args[i++];
            else if (option.equals("-password")) password = args[i++];
            else if (option.equals("-url")     ) jdbcUrl  = args[i++];
            else if (option.equals("-rowcount")) rowCount = Integer.parseInt(args[i++]);
            else if (option.equals("-?") | option.equals("-help")){
                System.out.println( "Valid options are :\n\t-driver\n\t-url\n\t-user\n\t-password\n\t-rowcount");
                System.exit(0);
            }
            else {System.out.println("Option " + option + " is ignored");i++;}
        }
        System.out.println( "Driver:  \t" + driverClassName);
        System.out.println( "Username:\t" + userName);
        System.out.println( "Password:\t" + password);
        System.out.println( "JDBC URL:\t" + jdbcUrl);
        System.out.println( "Row Count:\t" + rowCount);
        System.out.println();
        try{
            Class.forName(driverClassName).newInstance();
            con = DriverManager.getConnection( jdbcUrl, userName,password);
            System.out.println( con.getMetaData().getDriverName() + " " + con.getMetaData().getDriverVersion());
            System.out.println();
            createTestTable( con );
            test_InsertClassic( con );
            test_DeleteAll( con );
            test_InsertEmptyRows( con );
            test_DeleteRows( con );
            test_InsertRows( con );
            test_RowRequestPages( con );
            test_UpdateRows( con );
            test_UpdateRowsPrepare( con );
            test_UpdateRowsPrepareSP( con );
            test_UpdateRowsPrepareBatch( con );
            test_Scroll_getXXX( con );
            test_UpdateLargeBinary( con );
            test_UpdateLargeBinaryWithSP( con );
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            if (con != null){
                //dropTestTable( con );
                con.close();
            }
        }
    }
    
    
    
    /**
      *  1. Test
      *  Insert rows with default values with a classic insert statement.
      */  
    static void test_InsertClassic(Connection con){
        System.out.println();
        System.out.println( "Test insert rows with default values with a classic insert statement: " + rowCount + " rows");
        
        try{
            Statement st = con.createStatement( ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE );
            long time = -System.currentTimeMillis();
            for (int i=0; i<rowCount; i++){
                st.execute("INSERT INTO " + tableName + "(i) VALUES(" + i +")");
            }
            time += System.currentTimeMillis();
            ResultSet rs = st.executeQuery( "SELECT count(*) FROM " + tableName);
            rs.next();
            int count = rs.getInt(1);
            if (count != rowCount)
                System.out.println( "  Failed: Only " + count + " rows were inserted.");
            else System.out.println( "  Test time: " + time + " ms");
            st.close();
        }catch(Exception e){
            System.out.println("  Failed:"+e);
        }finally{
            System.out.println();
            System.out.println("===================================================================");
        }
    }
    
    
    
    /**
      *  2. Test
      *  Delete all rows with a single statement.
      */  
    static void test_DeleteAll(Connection con){
        System.out.println();
        System.out.println( "Test delete all rows: " + rowCount + " rows");
        
        try{
            long time = -System.currentTimeMillis();
            Statement st = con.createStatement();
            st.execute("DELETE FROM " + tableName);
            time += System.currentTimeMillis();
            System.out.println( "  Test time: " + time + " ms");
            st.close();
        }catch(Exception e){
            System.out.println("  Failed:"+e);
        }finally{
            System.out.println();
            System.out.println("===================================================================");
        }
    }
    
    
    
    /**
      *  3. Test
      *  Insert only empty rows with the default values of the row with the method insertRow().
      */  
    static void test_InsertEmptyRows(Connection con){
        System.out.println();
        System.out.println( "Test insert empty rows with insertRow(): " + rowCount + " rows");
        
        try{
            Statement st = con.createStatement( ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE );
            ResultSet rs = st.executeQuery("SELECT * FROM "+tableName);
            long time = -System.currentTimeMillis();
            for (int i=0; i<rowCount; i++){
                rs.moveToInsertRow();
                rs.insertRow();
            }
            time += System.currentTimeMillis();
            rs = st.executeQuery( "SELECT count(*) FROM " + tableName);
            rs.next();
            int count = rs.getInt(1);
            if (count != rowCount)
                 System.out.println( "  Failed: Only " + count + " rows were inserted.");
            else System.out.println( "  Test time: " + time + " ms");
            st.close();
        }catch(Exception e){
            System.out.println("  Failed:"+e);
        }finally{
            System.out.println();
            System.out.println("===================================================================");
        }
    }
    
    
    
    /**
      *  4. Test
      *  Delete rows with the method deleteRow().
      */  
    static void test_DeleteRows(Connection con){
        System.out.println();
        System.out.println( "Test delete rows with deleteRow(): " + rowCount + " rows");
        
        try{
            Statement st1 = con.createStatement();
            ResultSet rs = st1.executeQuery( "SELECT count(*) FROM " + tableName);
            rs.next();
            int count = rs.getInt(1);
            if (count != rowCount){
                // There are not the correct count of rows.
                if (count == 0){
                    createTestDataWithClassicInsert( con );
                    rs = st1.executeQuery( "SELECT count(*) FROM " + tableName);
                    rs.next();
                    count = rs.getInt(1);
                }
                if (count != rowCount){
                    System.out.println( "  Failed: Only " + (rowCount-count) + " rows were deleted.");
                    return;
                }
            }
            st1.close();
            
            Statement st = con.createStatement( ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE );
            rs = st.executeQuery("SELECT * FROM "+tableName);
            long time = -System.currentTimeMillis();
            for (int i=0; i<rowCount; i++){
                rs.next();
                rs.deleteRow();
            }
            time += System.currentTimeMillis();
            rs = st.executeQuery( "SELECT count(*) FROM " + tableName);
            rs.next();
            count = rs.getInt(1);
            if (count != 0)
                 System.out.println( "  Failed: Only " + (rowCount-count) + " rows were deleted.");
            else System.out.println( "  Test time: " + time + " ms");
            st.close();
        }catch(Exception e){
            System.out.println("  Failed:"+e);
        }finally{
            System.out.println();
            System.out.println("===================================================================");
        }
    }
    
    
    
    /**
      *  5. Test
      *  Insert rows with the method insertRow().
      */  
    static void test_InsertRows(Connection con){
        System.out.println();
        System.out.println( "Test insert rows with insertRow(): " + rowCount + " rows");
        
        try{
            Statement st = con.createStatement( ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE );
            ResultSet rs = st.executeQuery("SELECT * FROM " + tableName);
            long time = -System.currentTimeMillis();
            for (int i=0; i<rowCount; i++){
                rs.moveToInsertRow();
	            rs.updateBytes (  "bi", byteArray );
	            rs.updateString(  "c" , "Test" );
	            rs.updateDate  (  "d" , new Date( System.currentTimeMillis() ) );
	            rs.updateFloat (  "de", (float)1234.56789 );
	            rs.updateFloat (  "f" , (float)9876.54321 );
	            rs.updateBytes (  "im", largeByteArray );
	            rs.updateInt   (  "i" , i );
	            rs.updateDouble(  "m" , 23.45 );
	            rs.updateDouble(  "n" , 567.45 );
	            rs.updateFloat (  "r" , (float)78.89 );
	            rs.updateTime  (  "sd", new Time( System.currentTimeMillis() ) );
	            rs.updateShort (  "si", (short)i );
	            rs.updateFloat (  "sm", (float)34.56 );
	            rs.updateString(  "sy", "sysname (30) NULL" );
	            rs.updateString(  "t" , "ntext NULL, sample to save in the field" );
	            rs.updateByte  (  "ti", (byte)i );
	            rs.updateBytes (  "vb", byteArray );
	            rs.updateString(  "vc", "nvarchar (255) NULL" );
                rs.insertRow();
            }
            time += System.currentTimeMillis();
            rs = st.executeQuery( "SELECT count(*) FROM " + tableName);
            rs.next();
            int count = rs.getInt(1);
            if (count != rowCount){
                  st.execute("DELETE FROM " + tableName);
                  System.out.println( "  Failed: Only " + count + " rows were inserted.");
            }else System.out.println( "  Test time: " + time + " ms");
            st.close();
        }catch(Exception e){
        	e.printStackTrace();
            try{
                // reset for the next test
                Statement st = con.createStatement();
                st.execute("DELETE FROM " + tableName);
                st.close();
            }catch(Exception ee){/* ignore it */}
            System.out.println("  Failed:"+e);
        }finally{
            System.out.println();
            System.out.println("===================================================================");
        }
    }
    
    
    
    /**
      *  6. Test
      *  Request one page of rows from a large ResultSet.
      */  
    static void test_RowRequestPages(Connection con){
        int pages = 100; 
        int rows  = rowCount / pages;
        System.out.println();
        System.out.println( "Test request row pages : " + pages + " pages, " +rows + " rows per page");
        try{
            Statement st1 = con.createStatement();
            ResultSet rs = st1.executeQuery( "SELECT count(*) FROM " + tableName);
            rs.next();
            int count = rs.getInt(1);
            if (count != rowCount){
                // There are not the correct count of rows.
                if (count == 0){
                    createTestDataWithClassicInsert( con );
                    rs = st1.executeQuery( "SELECT count(*) FROM " + tableName);
                    rs.next();
                    count = rs.getInt(1);
                }
                if (count != rowCount){
                    System.out.println( "  Failed: Only " + (rowCount-count) + " rows were found.");
                    return;
                }
            }
            st1.close();
            
            long time = -System.currentTimeMillis();
            Statement st = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            st.setFetchSize( rows );
            for (int i=0; i<pages; i++){
                rs = st.executeQuery("SELECT * FROM " + tableName);
                rs.absolute( i*rows+1 );
                for (int r=1; r<rows; r++){
                    // only (rows-1) rows because absolute has already the first row
                    if (!rs.next()){
                        System.out.println( "  Failed: No rows were found at page " + i + " page and row " + r);
                        return;
                    }
                    int col_i = rs.getInt("i");
                    if (col_i != (i*rows+r)){
                        System.out.println( "  Failed: Wrong row " + col_i + ", it should be row " + (i*rows+r));
                        return;
                    }
                }
            }
            time += System.currentTimeMillis();
            System.out.println( "  Test time: " + time + " ms");
            st.close();
        }catch(Exception e){
            System.out.println("  Failed:"+e);
        }finally{
            System.out.println();
            System.out.println("===================================================================");
        }
    }

    
    
    /**
      *  7. Test
      *  Update rows with the method updateRow().
      */  
    static void test_UpdateRows(Connection con){
        System.out.println();
        System.out.println( "Test update rows with updateRow(): " + rowCount + " rows");
        
        try{
            Statement st = con.createStatement( ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE );
            ResultSet rs = st.executeQuery("SELECT * FROM " + tableName);
            int colCount = rs.getMetaData().getColumnCount();
            long time = -System.currentTimeMillis();
            int count = 0;
            while(rs.next()){
                for (int i=2; i<=colCount; i++){
                    rs.updateObject( i, rs.getObject(i) );
                }
                rs.updateRow();
                count++;
            }
            time += System.currentTimeMillis();
            if (count != rowCount)
                 System.out.println( "  Failed: Only " + count + " rows were updated.");
            else System.out.println( "  Test time: " + time + " ms");
            st.close();
        }catch(Exception e){
            System.out.println("  Failed:" + e);
        }finally{
            System.out.println();
            System.out.println("===================================================================");
        }
    }
    
    
    
    /**
      *  8. Test
      *  Update rows with a PreparedStatement.
      */  
    static void test_UpdateRowsPrepare(Connection con){
        System.out.println();
        System.out.println( "Test update rows with a PreparedStatement: " + rowCount + " rows");
        try{
            PreparedStatement pr = con.prepareStatement( "UPDATE " + tableName + " SET bi=?,c=?,d=?,de=?,f=?,im=?,i=?,m=?,n=?,r=?,sd=?,si=?,sm=?,sy=?,t=?,ti=?,vb=?,vc=? WHERE i=?" );
            long time = -System.currentTimeMillis();
            for (int i=0; i<rowCount; i++){
	            pr.setBytes (  1, byteArray );
	            pr.setString(  2 , "Test" );
	            pr.setDate  (  3 , new Date( System.currentTimeMillis() ) );
	            pr.setFloat (  4, (float)1234.56789 );
	            pr.setFloat (  5 , (float)9876.54321 );
	            pr.setBytes (  6, largeByteArray );
	            pr.setInt   (  7 , i );
	            pr.setDouble(  8 , 23.45 );
	            pr.setDouble(  9 , 567.45 );
	            pr.setFloat (  10 , (float)78.89 );
	            pr.setTime  (  11, new Time( System.currentTimeMillis() ) );
	            pr.setShort (  12, (short)23456 );
	            pr.setFloat (  13, (float)34.56 );
	            pr.setString(  14, "sysname (30) NULL" );
	            pr.setString(  15 , "text NULL" );
	            pr.setByte  (  16, (byte)28 );
	            pr.setBytes (  17, byteArray );
	            pr.setString(  18, "varchar (255) NULL" );
	            pr.setInt   (  19 , i );
                int updateCount = pr.executeUpdate();
                if (updateCount != 1){
                    System.out.println( "  Failed: Update count should be 1 but it is " + updateCount + ".");
                    return;
                }
            }
            time += System.currentTimeMillis();
            System.out.println( "  Test time: " + time + " ms");
            pr.close();
        }catch(Exception e){
            System.out.println("  Failed:"+e);
        }finally{
            System.out.println();
            System.out.println("===================================================================");
        }
    }
    
    
    
    /**
      *  9. Test
      *  Update rows with a PreparedStatement and a stored procedure.
      */  
    static void test_UpdateRowsPrepareSP(Connection con){
        System.out.println();
        System.out.println( "Test update rows with a PreparedStatement and a stored procedure: " + rowCount + " rows");
        
        try{
            Statement st = con.createStatement();
            try{st.execute("drop procedure sp_"+tableName);}catch(Exception e){/* ignore it */}
            st.execute("create procedure sp_"+tableName+" (@bi binary,@c nchar(255),@d datetime,@de decimal,@f float,@im image,@i int,@m money,@n numeric(18, 0),@r real,@sd smalldatetime,@si smallint,@sm smallmoney,@sy sysname,@t ntext,@ti tinyint,@vb varbinary(255),@vc nvarchar(255)) as UPDATE " + tableName + " SET bi=@bi,c=@c,d=@d,de=@de,f=@f,im=@im,i=@i,m=@m,n=@n,r=@r,sd=@sd,si=@si,sm=@sm,sy=@sy,t=@t,ti=@ti,vb=@vb,vc=@vc WHERE i=@i");

            PreparedStatement pr = con.prepareStatement( "exec sp_" + tableName + " ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?" );
            long time = -System.currentTimeMillis();
            for (int i=0; i<rowCount; i++){
	            pr.setBytes (  1, byteArray );
	            pr.setString(  2 , "Test" );
	            pr.setDate  (  3 , new Date( System.currentTimeMillis() ) );
	            pr.setFloat (  4, (float)1234.56789 );
	            pr.setFloat (  5 , (float)9876.54321 );
	            pr.setBytes (  6, largeByteArray );
	            pr.setInt   (  7 , i );
	            pr.setDouble(  8 , 23.45 );
	            pr.setDouble(  9 , 567.45 );
	            pr.setFloat (  10 , (float)78.89 );
	            pr.setTime  (  11, new Time( System.currentTimeMillis() ) );
	            pr.setShort (  12, (short)23456 );
	            pr.setFloat (  13, (float)34.56 );
	            pr.setString(  14, "sysname (30) NULL" );
	            pr.setString(  15 , "text NULL" );
	            pr.setByte  (  16, (byte)28 );
	            pr.setBytes (  17, byteArray );
	            pr.setString(  18, "varchar (255) NULL" );
                int updateCount = pr.executeUpdate();
                if (updateCount != 1){
                    System.out.println( "  Failed: Update count should be 1 but it is " + updateCount + ".");
                    return;
                }
            }
            time += System.currentTimeMillis();
            System.out.println( "  Test time: " + time + " ms");
            st.execute("drop procedure sp_"+tableName);
            st.close();
            pr.close();
        }catch(Exception e){
            System.out.println("  Failed:"+e);
        }finally{
            System.out.println();
            System.out.println("===================================================================");
        }
    }
    

    
    /**
      *  10. Test
      *  Update rows with a PreparedStatement and Batch.
      */  
    static void test_UpdateRowsPrepareBatch(Connection con){
        int batchSize = 10;
        int batches = rowCount / batchSize;
        System.out.println();
        System.out.println( "Test update rows with PreparedStatement and Batches: " + batches + " batches, " + batchSize + " batch size");
        
        try{
            PreparedStatement pr = con.prepareStatement( "UPDATE " + tableName + " SET bi=?,c=?,d=?,de=?,f=?,im=?,i=?,m=?,n=?,r=?,sd=?,si=?,sm=?,sy=?,t=?,ti=?,vb=?,vc=? WHERE i=?" );
            long time = -System.currentTimeMillis();
            for (int i=0; i<batches; i++){
                for (int r=0; r<batchSize; r++){
	                pr.setBytes (  1, byteArray );
	                pr.setString(  2 , "Test" );
	                pr.setDate  (  3 , new Date( System.currentTimeMillis() ) );
	                pr.setFloat (  4, (float)1234.56789 );
	                pr.setFloat (  5 , (float)9876.54321 );
	                pr.setBytes (  6, largeByteArray );
	                pr.setInt   (  7 , i*batchSize + r );
	                pr.setDouble(  8 , 23.45 );
	                pr.setDouble(  9 , 567.45 );
	                pr.setFloat (  10 , (float)78.89 );
	                pr.setTime  (  11, new Time( System.currentTimeMillis() ) );
	                pr.setShort (  12, (short)23456 );
	                pr.setFloat (  13, (float)34.56 );
	                pr.setString(  14, "sysname (30) NULL" );
	                pr.setString(  15 , "text NULL" );
	                pr.setByte  (  16, (byte)28 );
	                pr.setBytes (  17, byteArray );
	                pr.setString(  18, "varchar (255) NULL" );
	                pr.setInt   (  19 , i );
	                pr.addBatch();
	            }
                int[] updateCount = pr.executeBatch();
                if (updateCount.length != batchSize){
                    System.out.println( "  Failed: Update count size should be " + batchSize + " but it is " + updateCount.length + ".");
                    return;
                }
            }
            time += System.currentTimeMillis();
            System.out.println( "  Test time: " + time + " ms");
            pr.close();
        }catch(Exception e){
            System.out.println("  Failed:"+e);
        }finally{
            System.out.println();
            System.out.println("===================================================================");
        }
    }
     
    
    
    /**
      *  11. Test
      *  Scroll and call the getXXX methods for every columns.
      */  
    static void test_Scroll_getXXX(Connection con){
        System.out.println();
        System.out.println( "Test scroll and call the getXXX methods for every columns: " + rowCount + " rows");
        
        try{
            Statement st = con.createStatement();
            long time = -System.currentTimeMillis();
            ResultSet rs = st.executeQuery("SELECT * FROM " + tableName);
            for (int i=0; i<rowCount; i++){
                    rs.next();
	                rs.getInt   (  1 );
	                rs.getBytes (  2 );
	                rs.getString(  3 );
	                rs.getDate  (  4 );
	                rs.getFloat (  5 );
	                rs.getFloat (  6 );
	                rs.getBytes (  7 );
	                rs.getInt   (  8 );
	                rs.getDouble(  9 );
	                rs.getDouble(  10 );
	                rs.getFloat (  11 );
	                rs.getTime  (  12 );
	                rs.getShort (  13 );
	                rs.getFloat (  14 );
	                rs.getString(  15 );
	                rs.getString(  16 );
	                rs.getByte  (  17 );
	                rs.getBytes (  18 );
	                rs.getString(  19 );
            }
            time += System.currentTimeMillis();
            System.out.println( "  Test time: " + time + " ms");
            st.close();
        }catch(Exception e){
            System.out.println("  Failed:"+e);
        }finally{
            System.out.println();
            System.out.println("===================================================================");
        }
    }
     
    
    /**
      *  12. Test
      *  Update large binary data.
      */  
    static void test_UpdateLargeBinary(Connection con){
        System.out.println();
        System.out.println( "Test update large binary data: " + rowCount + "KB bytes");
        
        try{
            java.io.FileOutputStream fos = new java.io.FileOutputStream(tableName+".bin");
            byte bytes[] = new byte[1024];
            for(int i=0; i<rowCount; i++){
                fos.write(bytes);
            }
            fos.close();
            java.io.FileInputStream fis = new java.io.FileInputStream(tableName+".bin");
            long time = -System.currentTimeMillis();
            PreparedStatement pr = con.prepareStatement("Update " + tableName + " set im=? WHERE pr=1");
            pr.setBinaryStream( 1, fis, rowCount*1024 );
            pr.execute();
            pr.close();
            time += System.currentTimeMillis();
            System.out.println( "  Test time: " + time + " ms");
            fis.close();
            java.io.File file = new java.io.File(tableName+".bin");
            file.delete();
        }catch(Exception e){
            System.out.println("  Failed:"+e);
        }finally{
            System.out.println();
            System.out.println("===================================================================");
        }
    }
     
    

    
    /**
      *  12. Test
      *  Update large binary data with a SP.
      */  
    static void test_UpdateLargeBinaryWithSP(Connection con){
        System.out.println();
        System.out.println( "Test update large binary data with a SP: " + rowCount + "KB bytes");
        
        try{
            java.io.FileOutputStream fos = new java.io.FileOutputStream(tableName+".bin");
            byte bytes[] = new byte[1024];
            for(int i=0; i<rowCount; i++){
                fos.write(bytes);
            }
            fos.close();
            java.io.FileInputStream fis = new java.io.FileInputStream(tableName+".bin");
            long time = -System.currentTimeMillis();
            Statement st = con.createStatement();
            st.execute("CREATE PROCEDURE #UpdateLargeBinary(@im image) as Update " + tableName + " set im=@im WHERE pr=2");
            PreparedStatement pr = con.prepareStatement("exec #UpdateLargeBinary ?");
            pr.setBinaryStream( 1, fis, rowCount*1024 );
            pr.execute();
            st.execute("DROP PROCEDURE #UpdateLargeBinary");
            st.close();
            pr.close();
            time += System.currentTimeMillis();
            System.out.println( "  Test time: " + time + " ms");
            fis.close();
            java.io.File file = new java.io.File(tableName+".bin");
            file.delete();
        }catch(Exception e){
            System.out.println("  Failed:"+e);
        }finally{
            System.out.println();
            System.out.println("===================================================================");
        }
    }
     
    

    
    /**
      *  Create a new Table for testing
      */  
    static void createTestTable(Connection con) throws SQLException{
            Statement st;
            st = con.createStatement();
            //delete old table
            dropTestTable( con );

            //create table
            st.execute(
                "CREATE TABLE " + tableName + " ("+
	            "    pr  numeric IDENTITY,"+
	            "    bi  binary (255) NULL ,"+
	            "    c   nchar (255) NULL ,"+
	            "    d   datetime NULL ,"+
	            "    de  decimal(18, 0) NULL ,"+
	            "    f   float NULL ,"+
	            "    im  image NULL ,"+
	            "    i   int NULL ,"+
	            "    m   money NULL ,"+
	            "    n   numeric(18, 0) NULL ,"+
	            "    r   real NULL ,"+
	            "    sd  smalldatetime NULL ,"+
	            "    si  smallint NULL ,"+
	            "    sm  smallmoney NULL ,"+
	            "    sy  sysname NULL ,"+
	            "    t   ntext NULL ,"+
	            "    ti  tinyint NULL ,"+
	            "    vb  varbinary (255) NULL ,"+
	            "    vc  nvarchar (255) NULL, "+
	            "CONSTRAINT PK_BenchTest2 PRIMARY KEY CLUSTERED (pr) "+
	            ")");
	        st.close();  
    }
    

    
    static void deleteTestTable(Connection con){
        try{
            Statement st = con.createStatement();
            st.execute("DELETE FROM " + tableName);
            st.close();
        }catch(Exception e){/* ignore it */}
    }

    static void dropTestTable(Connection con){
        try{
            Statement st = con.createStatement();
            st.execute("drop table " + tableName);
            st.close();
        }catch(Exception e){/* ignore it */}
    }
    
    // create test data after the insert test is failed
    static void createTestDataWithClassicInsert(Connection con) throws SQLException{
        String sql = "INSERT INTO " + tableName + "(bi,c,d,de,f,im,i,m,n,r,si,sd,sm,sy,t,ti,vb,vc) VALUES(0x172243,'Test','20010101',1234.56789,9876.54321,0x";
        for(int i=0; i<largeByteArray.length; i++){
            sql += "00";
        }
        Statement st = con.createStatement();
        for (int i=0; i<rowCount; i++){
            st.execute(sql + ","+i+",23.45,567.45,78.89,"+i+",'11:11:11',34.56,'sysname (30) NULL','ntext NULL, sample to save in the field',"+(i & 0xFF)+",0x172243,'nvarchar (255) NULL')"  );
        }
        st.close();
    }
}