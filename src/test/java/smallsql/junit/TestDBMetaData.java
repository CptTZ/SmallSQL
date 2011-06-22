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
 * TestDBMetaData.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.junit;

import java.sql.*;
import java.text.*;
import java.util.Locale;
/**
 * @author Volker Berlin
 *
 */
public class TestDBMetaData extends BasicTestCase {

	public TestDBMetaData(){
		super();
	}
    
    
	public TestDBMetaData(String arg0) {
		super(arg0);
	}

    
    public void testGetURL() throws Exception{
        Connection con = AllTests.getConnection();
        DatabaseMetaData md = con.getMetaData();
        assertEquals( "URL", AllTests.JDBC_URL, md.getURL());
    }
    
    
    public void testVersions() throws Exception{
        Connection con = AllTests.getConnection();
        DatabaseMetaData md = con.getMetaData();
        assertEquals( "DriverVersion", md.getDriverVersion(), md.getDatabaseProductVersion());
        Driver driver = DriverManager.getDriver(AllTests.JDBC_URL);
        assertEquals( "MajorVersion", driver.getMajorVersion(), md.getDatabaseMajorVersion());
        assertEquals( "MajorVersion", driver.getMajorVersion(), md.getDriverMajorVersion());
        assertEquals( "MinorVersion", driver.getMinorVersion(), md.getDatabaseMinorVersion());
        assertEquals( "MinorVersion", driver.getMinorVersion(), md.getDriverMinorVersion());
        assertEquals( "Version", new DecimalFormat("###0.00", new DecimalFormatSymbols(Locale.US)).format(driver.getMajorVersion()+driver.getMinorVersion()/100.0), md.getDriverVersion());
        assertTrue( "jdbcCompliant", driver.jdbcCompliant() );
    }
    
    
    public void testFunctions() throws Exception{
        Connection con = AllTests.getConnection();
        DatabaseMetaData md = con.getMetaData();
        assertEquals( "getNumericFunctions", "ABS,ACOS,ASIN,ATAN,ATAN2,CEILING,COS,COT,DEGREES,EXP,FLOOR,LOG,LOG10,MOD,PI,POWER,RADIANS,RAND,ROUND,SIGN,SIN,SQRT,TAN,TRUNCATE",
                md.getNumericFunctions());
        assertEquals( "getStringFunctions", "ASCII,BIT_LENGTH,CHAR_LENGTH,CHARACTER_LENGTH,CHAR,CONCAT,DIFFERENCE,INSERT,LCASE,LEFT,LENGTH,LOCATE,LTRIM,OCTET_LENGTH,REPEAT,REPLACE,RIGHT,RTRIM,SOUNDEX,SPACE,SUBSTRING,TRIM,UCASE",
                md.getStringFunctions());
        assertEquals( "getStringFunctions", "IFNULL,USER,CONVERT,CAST,IIF",
                md.getSystemFunctions());
        assertEquals( "getStringFunctions", "CURDATE,CURRENT_DATE,CURTIME,DAYNAME,DAYOFMONTH,DAYOFWEEK,DAYOFYEAR,DAY,HOUR,MILLISECOND,MINUTE,MONTH,MONTHNAME,NOW,QUARTER,SECOND,TIMESTAMPADD,TIMESTAMPDIFF,WEEK,YEAR",
                md.getTimeDateFunctions());
    }
    
    
    public void testGetProcedures() throws Exception{
        Connection con = AllTests.getConnection();
        DatabaseMetaData md = con.getMetaData();
        ResultSet rs = md.getProcedures( null, null, "*");
        String[] colNames = {"PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "", "", "", "REMARKS", "PROCEDURE_TYPE"};
        int[] colTypes = {Types.NULL, Types.NULL, Types.NULL, Types.NULL, Types.NULL, Types.NULL, Types.NULL, Types.NULL };
        assertRSMetaData( rs, colNames, colTypes);
    }
    
    
    public void testGetProcedureColumns() throws Exception{
        Connection con = AllTests.getConnection();
        DatabaseMetaData md = con.getMetaData();
        ResultSet rs = md.getProcedureColumns( null, null, "*", null);
        String[] colNames = {"PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "COLUMN_NAME", "COLUMN_TYPE", "DATA_TYPE", "TYPE_NAME", "PRECISION", "LENGTH", "SCALE", "RADIX", "NULLABLE", "REMARKS" };
        int[] colTypes = {Types.NULL, Types.NULL, Types.NULL, Types.NULL, Types.NULL, Types.NULL, Types.NULL, Types.NULL, Types.NULL, Types.NULL, Types.NULL, Types.NULL, Types.NULL };
        assertRSMetaData( rs, colNames, colTypes);
    }
    
    
    public void testGetTables() throws Exception{
        String[] colNames = {"TABLE_CAT","TABLE_SCHEM","TABLE_NAME","TABLE_TYPE","REMARKS","TYPE_CAT","TYPE_SCHEM","TYPE_NAME","SELF_REFERENCING_COL_NAME","REF_GENERATION"};
        int[] types = {Types.VARCHAR, Types.NULL, Types.VARCHAR, Types.VARCHAR, Types.NULL, Types.NULL, Types.NULL, Types.NULL, Types.NULL, Types.NULL};
        
        //First test the function without a database connection
        Connection con = DriverManager.getConnection("jdbc:smallsql?");
        DatabaseMetaData md = con.getMetaData();
        ResultSet rs = md.getTables(null, null, null, null);
        super.assertRSMetaData(rs, colNames, new int[colNames.length]); //All types are NULL, because no row.
        assertFalse(rs.next());
        con.close();
        
        //Then test it with a database
        con = AllTests.getConnection();
        md = con.getMetaData();
        rs = md.getTables(null, null, null, null);
        super.assertRSMetaData(rs, colNames, types);
    }
    
    
    public void testGetSchemas() throws Exception{
        Connection con = AllTests.getConnection();
        DatabaseMetaData md = con.getMetaData();
        ResultSet rs = md.getSchemas();
        String[] colNames = {"TABLE_SCHEM"};
        int[] colTypes = {Types.NULL};
        assertRSMetaData( rs, colNames, colTypes);
        assertFalse(rs.next());
    }
    
    
	public void testGetCatalogs() throws Exception{
		Connection con = AllTests.getConnection();
		try{
			con.createStatement().execute("drop database test2\n\r\t");
		}catch(SQLException e){/* ignore it if the database already exists */}
		con.createStatement().execute("create database test2");
		DatabaseMetaData md = con.getMetaData();
		ResultSet rs = md.getCatalogs();
		assertRSMetaData( rs, new String[]{"TABLE_CAT"}, new int[]{Types.VARCHAR});
		while(rs.next()){
			System.out.println( "testCatalogs:"+rs.getObject(1) );
		}
	}
	
    
    public void testGetTableTypes() throws Exception{
        Connection con = AllTests.getConnection();
        DatabaseMetaData md = con.getMetaData();
        ResultSet rs = md.getTableTypes();
        String[] colNames = {"TABLE_TYPE"};
        int[] colTypes = {Types.VARCHAR};
        assertRSMetaData( rs, colNames, colTypes);
        String type = "";
        int count = 0;
        while(rs.next()){
            String type2 = rs.getString("TABLE_TYPE");
            assertTrue( type+"-"+type2, type.compareTo(type2)<0);
            type = type2;
            count++;
        }
        assertEquals("Table Type Count", 3, count);
    }
    
    
	public void testGetColumn() throws Exception{
		Connection con = AllTests.getConnection();
		dropTable(con,"tableColumns");
		dropView( con, "viewColumns");
		con.createStatement().execute("create table tableColumns(a int default 5)");
		DatabaseMetaData md = con.getMetaData();
		
		ResultSet rs = md.getColumns(null, null, "tableColumns", null);
        String[] colNames = {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE"};
        int[] colTypes = {Types.VARCHAR, Types.NULL, Types.VARCHAR, Types.VARCHAR, Types.SMALLINT, Types.VARCHAR, Types.INTEGER, Types.NULL, Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.NULL, Types.VARCHAR, Types.NULL, Types.NULL, Types.INTEGER, Types.INTEGER, Types.VARCHAR};
		assertRSMetaData( rs, colNames, colTypes);		
		assertTrue( "No row", rs.next() );
		assertEquals( "a", rs.getObject("COLUMN_NAME") ); 
		assertEquals( "INT", rs.getObject("TYPE_NAME") ); 
		assertEquals( "5", rs.getObject("COLUMN_Def") ); 
		
		con.createStatement().execute("create view viewColumns as Select * from tableColumns");
		
		rs = md.getColumns(null, null, "viewColumns", null);
		assertRSMetaData( rs, colNames, colTypes);		
		assertTrue( "No row", rs.next() );
		assertEquals( "a", rs.getObject("COLUMN_NAME") ); 
		assertEquals( "INT", rs.getObject("TYPE_NAME") ); 
		assertEquals( "5", rs.getObject("COLUMN_Def") ); 

		dropView( con, "viewColumns");
		dropTable( con, "tableColumns");
	}
    
    
    public void testGetTypeInfo() throws Exception{
        Connection con = AllTests.getConnection();
        DatabaseMetaData md = con.getMetaData();
        
        ResultSet rs = md.getTypeInfo();  
        
        String[] colNames = {"TYPE_NAME", "DATA_TYPE", "PRECISION", "LITERAL_PREFIX", "LITERAL_SUFFIX", "CREATE_PARAMS", "NULLABLE", "CASE_SENSITIVE", "SEARCHABLE", "UNSIGNED_ATTRIBUTE", "FIXED_PREC_SCALE", "AUTO_INCREMENT", "LOCAL_TYPE_NAME", "MINIMUM_SCALE", "MAXIMUM_SCALE", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "NUM_PREC_RADIX"};
        int[] colTypes = {Types.VARCHAR, Types.SMALLINT, Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.SMALLINT, Types.BOOLEAN, Types.SMALLINT, Types.BOOLEAN, Types.BOOLEAN, Types.BOOLEAN, Types.NULL, Types.INTEGER, Types.INTEGER, Types.NULL, Types.NULL, Types.NULL };
        assertRSMetaData(rs, colNames, colTypes);
        
        assertTrue(rs.next());
        int lastDataType = rs.getInt("data_type");
        while(rs.next()){
            int dataType = rs.getInt("data_type");
            assertTrue("Wrong sorting order", dataType>=lastDataType );
            lastDataType = dataType;
        }
    }
	
    
    public void testGetCrossReference() throws Exception{
        Connection con = AllTests.getConnection();
        dropTable(con,"tblCross1");
        dropTable(con,"tblCross2");
        DatabaseMetaData md = con.getMetaData();
        
        Statement st = con.createStatement();
        st.execute("Create Table tblCross1(id1 counter primary key, v nvarchar(100))");
        //st.execute("Create Table tblCross2(id2 counter foreign key REFERENCES tblCross1(id1), v nvarchar(100))");
        st.execute("Create Table tblCross2(id2 int , v nvarchar(100), foreign key (id2) REFERENCES tblCross1(id1))");
        String[] colNames = {"PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME", "DEFERRABILITY"};
        int[] colTypes = {Types.VARCHAR, Types.NULL, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.NULL, Types.VARCHAR, Types.VARCHAR, Types.SMALLINT, Types.SMALLINT, Types.SMALLINT, Types.VARCHAR, Types.VARCHAR, Types.SMALLINT };
        
        ResultSet rs = md.getCrossReference(null,null,"tblCross1",null,null,"tblCross2");
        assertRSMetaData(rs, colNames, colTypes);
        assertTrue(rs.next());
        assertFalse(rs.next());
        
        rs = md.getImportedKeys(null,null,"tblCross2");
        assertRSMetaData(rs, colNames, colTypes);
        assertTrue(rs.next());
        assertFalse(rs.next());
        
        rs = md.getExportedKeys(null,null,"tblCross1");
        assertRSMetaData(rs, colNames, colTypes);
        assertTrue(rs.next());
        assertFalse(rs.next());
        
        dropTable(con,"tblCross1");
        dropTable(con,"tblCross2");
    }
    
    
    public void testGetBestRowIdentifier() throws Exception{
        Connection con = AllTests.getConnection();
        dropTable(con,"tblBestRow1");
        DatabaseMetaData md = con.getMetaData();
        Statement st = con.createStatement();
        st.execute("Create Table tblBestRow1(id1 counter primary key, v nvarchar(100))");
        String[] colNames = {"SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "PSEUDO_COLUMN"};
        int[] colTypes = {Types.SMALLINT, Types.VARCHAR, Types.INTEGER, Types.VARCHAR, Types.INTEGER, Types.NULL, Types.SMALLINT, Types.SMALLINT};
        
        ResultSet rs = md.getBestRowIdentifier(null, null, "tblBestRow1", DatabaseMetaData.bestRowSession, true);        
        assertRSMetaData(rs, colNames, colTypes);
        assertTrue(rs.next());
        assertEquals("Columnname:", "id1", rs.getString("COLUMN_NAME"));
        assertFalse(rs.next());
        
        String[] colNames2 = {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME"};
        int[] colTypes2 = {Types.VARCHAR, Types.NULL, Types.VARCHAR, Types.VARCHAR, Types.SMALLINT, Types.VARCHAR};
        rs = md.getPrimaryKeys(null, null, "tblBestRow1");        
        assertRSMetaData(rs, colNames2, colTypes2);
        assertTrue(rs.next());
        assertEquals("Columnname:", "id1", rs.getString("COLUMN_NAME"));
        assertFalse(rs.next());
        
        String[] colNames3 = {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "NON_UNIQUE", "INDEX_QUALIFIER", "INDEX_NAME", "TYPE", "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC", "CARDINALITY", "PAGES", "FILTER_CONDITION"};
        int[] colTypes3 = {Types.VARCHAR, Types.NULL, Types.VARCHAR, Types.BOOLEAN, Types.NULL, Types.VARCHAR, Types.SMALLINT, Types.SMALLINT, Types.VARCHAR, Types.NULL, Types.NULL, Types.NULL, Types.NULL};
        rs = md.getIndexInfo(null, null, "tblBestRow1", true, true);        
        assertRSMetaData(rs, colNames3, colTypes3);
        assertTrue(rs.next());
        assertEquals("Columnname:", "id1", rs.getString("COLUMN_NAME"));
        assertFalse(rs.next());
        
        dropTable(con,"tblBestRow1");
    }
    
    
    public void testGetgetUDTs() throws Exception{
        Connection con = AllTests.getConnection();
        DatabaseMetaData md = con.getMetaData();
        ResultSet rs = md.getUDTs(null, null, null, null);
        String[] colNames = {"TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "CLASS_NAME", "DATA_TYPE", "REMARKS"};
        int[] colTypes = new int[colNames.length];
        assertRSMetaData( rs, colNames, colTypes);
        assertFalse(rs.next());
    }
    
    
    public void testGetConnection() throws Exception{
        Connection con = AllTests.getConnection();
        DatabaseMetaData md = con.getMetaData();
        assertEquals(con, md.getConnection());
    }

    
}
