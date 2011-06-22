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
 * TestJoins.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.junit;

import junit.framework.*;

import java.math.BigDecimal;
import java.sql.*;

public class TestJoins extends BasicTestCase {

    private TestValue testValue;

    private static final String table = "table_joins";
    private static final String table2= "table_joins2";
	private static final String table3= "table_joins3";

    private static final TestValue[] TESTS = new TestValue[]{
        a("tinyint"           , new Byte( (byte)3),     new Byte( (byte)4)),
        a("byte"              , new Byte( (byte)3),     new Byte( (byte)4)),
        a("smallint"          , new Short( (short)3),   new Short( (short)4)),
        a("int"               , new Integer(3),         new Integer(4)),
        a("bigint"            , new Long(3),            new Long(4)),
        a("real"              , new Float(3.45),        new Float(4.56)),
        a("float"             , new Float(3.45),        new Float(4.56)),
        a("double"            , new Double(3.45),       new Double(4.56)),
        a("smallmoney"        , new Float(3.45),        new Float(4.56)),
        a("money"             , new Float(3.45),        new Float(4.56)),
        a("money"             , new Double(3.45),       new Double(4.56)),
        a("numeric(19,2)"     , new BigDecimal("3.45"), new BigDecimal("4.56")),
        a("decimal(19,2)"     , new BigDecimal("3.45"), new BigDecimal("4.56")),
        a("varnum(28,2)"      , new BigDecimal(3.45),   new BigDecimal(4.56)),
        a("number(28,2)"      , new BigDecimal(3.45),   new BigDecimal(4.56)),
        a("varchar(100)"      , new String("abc"),      new String("qwert")),
        a("nvarchar(100)"     , new String("abc"),      new String("qwert")),
        a("varchar2(100)"     , new String("abc"),      new String("qwert")),
        a("nvarchar2(100)"    , new String("abc"),      new String("qwert")),
        a("character(100)"    , new String("abc"),      new String("qwert")),
        a("char(100)"         , new String("abc"),      new String("qwert")),
        a("nchar(100)"        , new String("abc"),      new String("qwert")),
        a("text"              , new String("abc"),      new String("qwert")),
        a("ntext"             , new String("abc"),      new String("qwert")),
        a("date"              , new Date(99, 1,1),      new Date(99, 2,2)),
        a("time"              , new Time(9, 1,1),       new Time(9, 2,2)),
        a("timestamp"         , new Timestamp(99, 1,1,0,0,0,0),      new Timestamp(99, 2,2,0,0,0,0)),
        a("datetime"          , new Timestamp(99, 1,1,0,0,0,0),      new Timestamp(99, 2,2,0,0,0,0)),
        a("smalldatetime"     , new Timestamp(99, 1,1,0,0,0,0),      new Timestamp(99, 2,2,0,0,0,0)),
        a("binary(100)"       , new byte[]{12, 45, 1},  new byte[]{12, 45, 2, 56, 89}),
        a("varbinary(100)"    , new byte[]{12, 45, 1},  new byte[]{12, 45, 2, 56, 89}),
        a("raw(100)"          , new byte[]{12, 45, 1},  new byte[]{12, 45, 2, 56, 89}),
        a("long raw"          , new byte[]{12, 45, 1},  new byte[]{12, 45, 2, 56, 89}),
        a("longvarbinary"     , new byte[]{12, 45, 1},  new byte[]{12, 45, 2, 56, 89}),
        a("blob"              , new byte[]{12, 45, 1},  new byte[]{12, 45, 2, 56, 89}),
        a("image"             , new byte[]{12, 45, 1},  new byte[]{12, 45, 2, 56, 89}),
        a("boolean"           , Boolean.FALSE,          Boolean.TRUE),
        a("bit"               , Boolean.FALSE,          Boolean.TRUE),
        a("uniqueidentifier"  , "12345678-3445-3445-3445-1234567890ab",      "12345679-3445-3445-3445-1234567890ab"),
    };


    TestJoins(TestValue testValue){
        super(testValue.dataType);
        this.testValue = testValue;
    }

	
	private void clear() throws SQLException{
        Connection con = AllTests.getConnection();
        dropTable( con, table );
        dropTable( con, table2 );
        dropTable( con, table3 );
	}
	
	
    public void tearDown() throws SQLException{
		clear();
    }

    public void setUp() throws Exception{
		clear();
        Connection con = AllTests.getConnection();
        Statement st = con.createStatement();
        st.execute("create table " + table + "(a " + testValue.dataType +" PRIMARY KEY, b " + testValue.dataType + ")");
        st.execute("create table " + table2+ "(c " + testValue.dataType +" PRIMARY KEY, d " + testValue.dataType + ")");
		st.execute("create table " + table3+ "(c " + testValue.dataType +" PRIMARY KEY, d " + testValue.dataType + ")");
        st.close();
		con.close();
		con = AllTests.getConnection();
        PreparedStatement pr = con.prepareStatement("INSERT into " + table + "(a,b) Values(?,?)");
	    insertValues( pr );
        pr.close();

	    pr = con.prepareStatement("INSERT into " + table2 + " Values(?,?)");
	    insertValues( pr );
        pr.close();
    }

    private void insertValues(PreparedStatement pr ) throws Exception{
            pr.setObject( 1, testValue.small);
            pr.setObject( 2, testValue.large);
            pr.execute();

            pr.setObject( 1, testValue.small);
            pr.setObject( 2, testValue.small);
            pr.execute();

            pr.setObject( 1, testValue.large);
            pr.setObject( 2, testValue.large);
            pr.execute();

            pr.setObject( 1, testValue.large);
            pr.setObject( 2, testValue.small);
            pr.execute();

            pr.setObject( 1, null);
            pr.setObject( 2, testValue.small);
            pr.execute();

            pr.setObject( 1, testValue.small);
            pr.setObject( 2, null);
            pr.execute();

            pr.setObject( 1, null);
            pr.setObject( 2, null);
            pr.execute();
    }

    public void runTest() throws Exception{
        Connection con = AllTests.getConnection();
        Statement st = con.createStatement();
        ResultSet rs;

        rs = st.executeQuery("Select * from " + table + " where 1 = 0");
        assertFalse( "To many rows", rs.next() );

        assertRowCount( 7, "Select * from " + table);
        assertRowCount( 49, "Select * from " + table + " t1, " + table2 + " t2");
        assertRowCount( 0, "Select * from " + table + ", " + table3);
        assertRowCount( 49, "Select * from ("+ table +"), " + table2);
        assertRowCount( 49, "Select * from " + table + " Cross Join " + table2);
        assertRowCount( 13, "Select * from " + table + " INNER JOIN " + table2 + " ON " + table + ".a = " + table2 + ".c");
        assertRowCount( 13, "Select * from " + table + "       JOIN " + table2 + " ON " + table2 + ".c = " + table + ".a");
        assertRowCount( 13, "Select * from {oj " + table + " INNER JOIN " + table2 + " ON " + table + ".a = " + table2 + ".c}");
        assertRowCount( 13, "Select * from " + table + " AS t1 INNER JOIN " + table2 + " t2 ON t1.a = t2.c");
        assertRowCount( 13, "Select * from {oj " + table + " t1 INNER JOIN " + table2 + " t2 ON t1.a = t2.c}");
        assertRowCount( 4, "Select * from " + table + " t1 INNER JOIN " + table2 + " t2 ON t1.a = t2.c and t1.b=t2.d");
        assertRowCount( 4, "Select * from " + table + " t1       JOIN " + table2 + " t2 ON t1.a = t2.c and t2.d=t1.b");
        assertRowCount( 7, "Select * from " + table + " t1 LEFT OUTER JOIN " + table2 + " t2 ON t1.a = t2.c and t1.b=t2.d");
        assertRowCount( 7, "Select * from " + table + " t1 LEFT       JOIN " + table2 + " t2 ON t1.a = t2.c and t1.b=t2.d");
        assertRowCount( 15, "Select * from " + table + " t1 LEFT OUTER JOIN " + table2 + " t2 ON t1.a = t2.c");
        assertRowCount( 7, "Select * from " + table + " t1 LEFT OUTER JOIN " + table3 + " t2 ON t1.a = t2.c");
        assertRowCount( 7, "Select * from " + table + " t1 RIGHT OUTER JOIN " + table2 + " t2 ON t1.a = t2.c and t1.b=t2.d");
        assertRowCount( 7, "Select * from " + table + " t1 RIGHT OUTER JOIN " + table2 + " t2 ON false");
        assertRowCount( 15, "Select * from " + table + " t1 RIGHT OUTER JOIN " + table2 + " t2 ON t1.a = t2.c");
        assertRowCount( 0, "Select * from " + table + " t1 RIGHT OUTER JOIN " + table3 + " t2 ON t1.a = t2.c");
        assertRowCount( 14, "Select * from " + table + " t1 FULL OUTER JOIN " + table2 + " t2 ON 1=0");
        assertRowCount( 17, "Select * from " + table + " t1 FULL OUTER JOIN " + table2 + " t2 ON t1.a = t2.c");
		assertRowCount( 7, "Select * from " + table + " t1 FULL OUTER JOIN " + table3 + " t2 ON t1.a = t2.c");
		assertRowCount( 7, "Select * from " + table3 + " t1 FULL OUTER JOIN " + table + " t2 ON t1.c = t2.a");
        assertRowCount( 5, "Select * from " + table + " INNER JOIN (SELECT DISTINCT c FROM " + table2 + ") t1 ON " + table + ".a = t1.c");
        
        st.close();
    }

    public static Test suite() throws Exception{
        TestSuite theSuite = new TestSuite("Joins");
        for(int i=0; i<TESTS.length; i++){
            theSuite.addTest(new TestJoins( TESTS[i] ) );
        }
        return theSuite;
    }


    private static TestValue a(String dataType, Object small, Object large){
        TestValue value = new TestValue();
        value.dataType  = dataType;
        value.small     = small;
        value.large     = large;
        return value;
    }

    private static class TestValue{
        String dataType;
        Object small;
        Object large;
    }

}