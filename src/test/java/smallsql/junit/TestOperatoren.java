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
 * TestOperatoren.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.junit;

import junit.framework.*;
import java.sql.*;
import java.math.*;

public class TestOperatoren extends BasicTestCase {

    private TestValue testValue;

    private static final String table = "table_functions";

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
        a("varnum(28,2)"      , new BigDecimal("2.34"), new BigDecimal("3.45")),
        a("number(28,2)"      , new BigDecimal("2.34"), new BigDecimal("3.45")),
        a("varchar(100)"      , new String("abc"),      new String("qwert")),
        a("varchar(60000)"    , new String(new char[43210]),      new String("qwert")),
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
        a("varbinary(60000)"  , new byte[54321],        new byte[]{12, 45, 2, 56, 89}),
        a("raw(100)"          , new byte[]{12, 45, 1},  new byte[]{12, 45, 2, 56, 89}),
        a("long raw"          , new byte[]{12, 45, 1},  new byte[]{12, 45, 2, 56, 89}),
        a("longvarbinary"     , new byte[]{12, 45, 1},  new byte[]{12, 45, 2, 56, 89}),
        a("blob"              , new byte[]{12, 45, 1},  new byte[]{12, 45, 2, 56, 89}),
        a("image"             , new byte[]{12, 45, 1},  new byte[]{12, 45, 2, 56, 89}),
        a("boolean"           , Boolean.FALSE,          Boolean.TRUE),
        a("bit"               , Boolean.FALSE,          Boolean.TRUE),
        a("uniqueidentifier"  , "12345678-3445-3445-3445-1234567890ab",      "12345679-3445-3445-3445-1234567890ac"),
    };


    TestOperatoren(TestValue testValue){
        super(testValue.dataType);
        this.testValue = testValue;
    }

    public void tearDown(){
        try{
            Connection con = AllTests.getConnection();
            Statement st = con.createStatement();
            st.execute("drop table " + table);
            st.close();
        }catch(Throwable e){
            //e.printStackTrace();
        }
    }

    public void setUp(){
        tearDown();
        try{
            Connection con = AllTests.getConnection();
            Statement st = con.createStatement();
            st.execute("create table " + table + "(a " + testValue.dataType +", b " + testValue.dataType + ")");
            st.close();
            PreparedStatement pr = con.prepareStatement("INSERT into " + table + "(a,b) Values(?,?)");

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
            pr.close();
        }catch(Throwable e){
            e.printStackTrace();
        }
    }


    public void runTest() throws Exception{
        Connection con = AllTests.getConnection();
        Statement st = con.createStatement();
        ResultSet rs;

        rs = st.executeQuery("Select * from " + table + " where 1 = 0");
        assertFalse( "To many rows", rs.next() );

        rs = st.executeQuery("Select * from " + table + " where a = b");
        assertTrue( "To few rows", rs.next() );
        assertEqualsObject( "Values not equals", rs.getObject(1), rs.getObject(2), false);
        assertTrue( "To few rows", rs.next() );
        assertEqualsObject( "Values not equals", rs.getObject(1), rs.getObject(2), false);
        assertFalse( "To many rows", rs.next() );

        rs = st.executeQuery("Select * from " + table + " where a <= b and b <= a");
        assertTrue( "To few rows", rs.next() );
        assertEqualsObject( "Values not equals", rs.getObject(1), rs.getObject(2), false);
        assertTrue( "To few rows", rs.next() );
        assertEqualsObject( "Values not equals", rs.getObject(1), rs.getObject(2), false);
        assertFalse( "To many rows", rs.next() );

        rs = st.executeQuery("Select * from " + table + " where (a > (b))");
        assertTrue( "To few rows", rs.next() );
        assertFalse( "To many rows", rs.next() );

        rs = st.executeQuery("Select * from " + table + " where a >= b");
        assertTrue( "To few rows", rs.next() );
        assertTrue( "To few rows", rs.next() );
        assertTrue( "To few rows", rs.next() );
        assertFalse( "To many rows", rs.next() );

        rs = st.executeQuery("Select * from " + table + " where not (a >= b)");
        assertTrue( "To few rows", rs.next() );
        assertTrue( "To few rows", rs.next() );
        assertTrue( "To few rows", rs.next() );
        assertTrue( "To few rows", rs.next() );
        assertFalse( "To many rows", rs.next() );

        rs = st.executeQuery("Select * from " + table + " where a < b");
        assertTrue( "To few rows", rs.next() );
        assertFalse( "To many rows", rs.next() );

        rs = st.executeQuery("Select * from " + table + " where a < b or a>b");
        assertTrue( "To few rows", rs.next() );
        assertTrue( "To few rows", rs.next() );
        assertFalse( "To many rows", rs.next() );

        rs = st.executeQuery("Select * from " + table + " where a <= b");
        assertTrue( "To few rows", rs.next() );
        assertTrue( "To few rows", rs.next() );
        assertTrue( "To few rows", rs.next() );
        assertFalse( "To many rows", rs.next() );

        rs = st.executeQuery("Select * from " + table + " where a <> b");
        assertTrue( "To few rows", rs.next() );
        assertTrue( "To few rows", rs.next() );
        assertFalse( "To many rows", rs.next() );

        PreparedStatement pr = con.prepareStatement("Select * from " + table + " where a between ? and ?");
        pr.setObject( 1, testValue.small);
        pr.setObject( 2, testValue.large);
        rs = pr.executeQuery();
        assertTrue( "To few rows", rs.next() );
        assertTrue( "To few rows", rs.next() );
        assertTrue( "To few rows", rs.next() );
        assertTrue( "To few rows", rs.next() );
        assertTrue( "To few rows", rs.next() );
        assertFalse( "To many rows", rs.next() );
		pr.close();

		pr = con.prepareStatement("Select * from " + table + " where a not between ? and ?");
		pr.setObject( 1, testValue.small);
		pr.setObject( 2, testValue.large);
		rs = pr.executeQuery();
		assertTrue( "To few rows", rs.next() );
		assertTrue( "To few rows", rs.next() );
		assertFalse( "To many rows", rs.next() );
		pr.close();

		pr = con.prepareStatement("Select * from " + table + " where a in(?,?)");
		pr.setObject( 1, testValue.small);
		pr.setObject( 2, testValue.large);
		rs = pr.executeQuery();
		assertTrue( "To few rows", rs.next() );
		assertTrue( "To few rows", rs.next() );
		assertTrue( "To few rows", rs.next() );
        assertTrue( "To few rows", rs.next() );
		assertTrue( "To few rows", rs.next() );
		assertFalse( "To many rows", rs.next() );
		pr.close();

		pr = con.prepareStatement("Select * from " + table + " where a not in(?,?)");
		pr.setObject( 1, testValue.small);
		pr.setObject( 2, testValue.large);
		rs = pr.executeQuery();
		assertTrue( "To few rows", rs.next());
		assertTrue( "To few rows", rs.next());
		assertFalse( "To many rows", rs.next() );
		pr.close();

        st.close();
    }

    public static Test suite() throws Exception{
        TestSuite theSuite = new TestSuite("Operatoren");
        for(int i=0; i<TESTS.length; i++){
            theSuite.addTest(new TestOperatoren( TESTS[i] ) );
        }
        return theSuite;
    }

    public static void main(String[] argv) {
        junit.swingui.TestRunner.main(new String[]{TestOperatoren.class.getName()});
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