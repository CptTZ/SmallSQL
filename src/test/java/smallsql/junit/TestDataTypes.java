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
 * TestDataTypes.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.junit;

import junit.framework.*;
import java.sql.*;
import java.math.*;

public class TestDataTypes extends BasicTestCase{

    static final String[] DATATYPES = { "varchar(100)",
                                                "varchar2(130)", "nvarchar(137)", "nvarchar2(137)", "sysname",
                                                "char(100)", "CHARACTER(99)",
                                                "nchar(80)",
                                                "int", "smallint", "tinyint", "bigint", "byte",
                                                "real", "float", "double",
                                                "bit", "Boolean",
                                                "binary( 125 )", "varbinary(57)", "raw(88)",
                                                "java_object", "sql_variant",
                                                "image", "LONGvarbinary", "long raw",
                                                "blob", "clob","nclob",
                                                "text", "ntext", "LongVarchar", "long",
                                                "time", "date", "datetime", "timestamp", "SMALLDATETIME",
                                                "UNIQUEIDENTIFIER",
                                                "numeric(28,4)", "decimal(29,4)","number(29,4)", "varnum(29,4)",
                                                "COUNTER",
                                                "money", "smallmoney"};

    private static final String table = "table_datatypes";

    private String datatype;

    TestDataTypes( String datatype ){
        super( datatype );
        this.datatype = datatype;
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
    }

    public void runTest() throws Throwable {
        Connection con = AllTests.getConnection();
        Statement st = con.createStatement();
        st.execute("Create Table " + table +"(abc " + datatype + ")");
        String name = "abc";

        Object[] values = null;
        String   quote = "";
        String escape1 = "";
        String escape2 = "";
        boolean needTrim = false;

        ResultSet rs = st.executeQuery("SELECT * From " + table);
		ResultSetMetaData md = rs.getMetaData();
        switch(md.getColumnType(1)){
            case Types.CHAR:
                needTrim = true;
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.CLOB:
                values = new Object[]{null,"qwert", "asdfg", "hjhjhj", "1234567890 qwertzuiop 1234567890 asdfghjklö 1234567890 yxcvbnm,.- 1234567890 "};
                quote  = "\'";
                break;
            case Types.BIGINT:
                values = new Object[]{null,new Long(123), new Long(-2123), new Long(392839283)};
                break;
            case Types.INTEGER:
                values = new Object[]{null,new Integer(123), new Integer(-2123), new Integer(392839283)};
                break;
            case Types.SMALLINT:
                values = new Object[]{null,new Integer(123), new Integer(-2123), new Integer(32000)};
                break;
            case Types.TINYINT:
                values = new Object[]{null,new Integer(0), new Integer(12), new Integer(228)};
                break;
            case Types.REAL:
                values = new Object[]{null,new Float(0.0), new Float(-12.123), new Float(22812345234.9)};
                break;
            case Types.FLOAT:
            case Types.DOUBLE:
                values = new Object[]{null,new Double(0.0), new Double(-12.123), new Double(22812345234.9)};
                break;
            case Types.NUMERIC:
            case Types.DECIMAL:
                needTrim = true;
            	if(md.getPrecision(1)<16){//smallmoney
					values = new Object[]{null,new BigDecimal("0.0"), new BigDecimal("-2"), new BigDecimal("-12.123")};
                /*if(rs.getMetaData().isCurrency(1)){
                    values = new Object[]{null, new Money(0.0), new Money(-12.123), new Money(202812.9)};*/
                }else{
                    values = new Object[]{null,new BigDecimal("0.0"), new BigDecimal("-2"), new BigDecimal("-12.123"), new BigDecimal("22812345234.9")};
                }
                break;
            case Types.BIT:
            case Types.BOOLEAN:
                values = new Object[]{null, Boolean.TRUE, Boolean.FALSE};
                break;
            case Types.TIME:
                values = new Object[]{null, new Time(10,17,56), new Time(0,0,0),new Time(23,59,59)};
                escape1 = "{t '";
                escape2 = "'}";
                break;
			case Types.DATE:
				values = new Object[]{null, new java.sql.Date(10,10,1), new java.sql.Date(0,0,1),new java.sql.Date(70,0,1)};
				escape1 = "{d '";
				escape2 = "'}";
				break;
			case Types.TIMESTAMP:
				if(md.getPrecision(1) >16)
					values = new Object[]{null, new Timestamp(10,10,1, 10,17,56, 0), new Timestamp(0,0,1, 0,0,0, 0),new Timestamp( 120,1,1, 23,59,59, 500000000),new Timestamp(0),new Timestamp( -120,1,1, 23,59,59, 500000000)};
				else//smalldatetime
					values = new Object[]{null, new Timestamp(10,10,1, 10,17,0, 0), new Timestamp(0,0,1, 0,0,0, 0),new Timestamp(0)};
				escape1 = "{ts '";
				escape2 = "'}";
				break;
            case Types.BINARY:
                needTrim = true;
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                values = new Object[]{null, new byte[]{1, 127, -23}};
                break;
            case Types.JAVA_OBJECT:
                values = new Object[]{null, new Integer(-123), new Double(1.2), new byte[]{1, 127, -23}};
                break;
            case -11: //UNIQUEIDENTIFER
                values = new Object[]{null, "342734E3-D9AC-408F-8724-B7A257C4529E", "342734E3-D9AC-408F-8724-B7A257C4529E"};
                quote  = "\'";
                break;
            default: fail("Unknown column type: " + rs.getMetaData().getColumnType(1));
        }
        rs.close();
		
		// remove all resource for reloading the tables from file
		con.close();
		con = AllTests.getConnection();
		st = con.createStatement();

        for(int i=0; i<values.length; i++){
            Object val = values[i];
            String q = (val == null) ? "" : quote;
            String e1 = (val == null) ? "" : escape1;
            String e2 = (val == null) ? "" : escape2;
            if(val instanceof byte[]){
                StringBuffer buf = new StringBuffer( "0x" );
                for(int k=0; k<((byte[])val).length; k++){
                    String digit = "0" + Integer.toHexString( ((byte[])val)[k] );
                    buf.append( digit.substring( digit.length()-2 ) );
                }
                val = buf.toString();
            }
            st.execute("Insert into " + table + "(abc) Values(" + e1 + q + val + q + e2 + ")");
        }
		checkValues( st, values, needTrim);
		
		st.execute("Delete From "+ table);
		CallableStatement cal = con.prepareCall("Insert Into " + table + "(abc) Values(?)");
        for(int i=0; i<values.length; i++){
            Object val = values[i];
			cal.setObject( 1, val);
			cal.execute();
        }
		cal.close();
		checkValues( st, values, needTrim);
		
		st.execute("Delete From "+ table);
		cal = con.prepareCall("Insert Into " + table + "(abc) Values(?)");
        for(int i=0; i<values.length; i++){
            Object val = values[i];
			if(val == null){
				cal.setNull( 1, Types.NULL );
			}else
			if(val instanceof Time){
				cal.setTime( 1, (Time)val );
			}else
			if(val instanceof Timestamp){
				cal.setTimestamp( 1, (Timestamp)val );
			}else
			if(val instanceof Date){
				cal.setDate( 1, (Date)val );
			}else
			if(val instanceof String){
				cal.setString( 1, (String)val );
			}else
			if(val instanceof Boolean){
				cal.setBoolean( 1, ((Boolean)val).booleanValue() );
			}else
			if(val instanceof Byte){
				cal.setByte( 1, ((Byte)val).byteValue() );
			}else
			if(val instanceof Short){
				cal.setShort( 1, ((Short)val).shortValue() );
			}else
			if(val instanceof Integer){
				cal.setInt( 1, ((Integer)val).intValue() );
			}else
			if(val instanceof Long){
				cal.setLong( 1, ((Long)val).longValue() );
			}else
			if(val instanceof Float){
				cal.setFloat( 1, ((Float)val).floatValue() );
			}else
			if(val instanceof Double){
				cal.setDouble( 1, ((Double)val).doubleValue() );
			}else
			if(val instanceof BigDecimal){
				cal.setBigDecimal( 1, (BigDecimal)val );
			}else
			if(val instanceof byte[]){
				cal.setBytes( 1, (byte[])val );
			}
			cal.execute();
        }
		cal.close();
		checkValues( st, values, needTrim);

        
        st.execute("Delete From "+ table);
        Statement st2 = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs2 = st2.executeQuery("SELECT * From " + table);
        for(int i=0; i<values.length; i++){
            rs2.moveToInsertRow();
            Object val = values[i];
            if(val == null){
                rs2.updateNull( name );
            }else
            if(val instanceof Time){
                rs2.updateTime( name, (Time)val );
            }else
            if(val instanceof Timestamp){
                rs2.updateTimestamp( name, (Timestamp)val );
            }else
            if(val instanceof Date){
                rs2.updateDate( name, (Date)val );
            }else
            if(val instanceof String){
                rs2.updateString( name, (String)val );
            }else
            if(val instanceof Boolean){
                rs2.updateBoolean( name, ((Boolean)val).booleanValue() );
            }else
            if(val instanceof Byte){
                rs2.updateByte( name, ((Byte)val).byteValue() );
            }else
            if(val instanceof Short){
                rs2.updateShort( name, ((Short)val).shortValue() );
            }else
            if(val instanceof Integer){
                rs2.updateInt( name, ((Integer)val).intValue() );
            }else
            if(val instanceof Long){
                rs2.updateLong( name, ((Long)val).longValue() );
            }else
            if(val instanceof Float){
                rs2.updateFloat( name, ((Float)val).floatValue() );
            }else
            if(val instanceof Double){
                rs2.updateDouble( name, ((Double)val).doubleValue() );
            }else
            if(val instanceof BigDecimal){
                rs2.updateBigDecimal( name, (BigDecimal)val );
            }else
            if(val instanceof byte[]){
                rs2.updateBytes( name, (byte[])val );
            }
            rs2.insertRow();
        }
        st2.close();
        checkValues( st, values, needTrim);
    }
	
	
	private void checkValues(Statement st, Object[] values, boolean needTrim) throws Exception{
        ResultSet rs = st.executeQuery("SELECT * From " + table);

        int i = 0;
        while(rs.next()){
            assertEqualsRsValue(values[i], rs, needTrim);
            i++;
        }
        rs.close();
	}


    public static Test suite() throws Exception{
        TestSuite theSuite = new TestSuite("Data Types");
        for(int i=0; i<DATATYPES.length; i++){
            theSuite.addTest(new TestDataTypes( DATATYPES[i] ) );
        }
        return theSuite;
    }

    public static void main(String[] argv) {
        junit.swingui.TestRunner.main(new String[]{TestDataTypes.class.getName()});
    }
}