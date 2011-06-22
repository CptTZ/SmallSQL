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
 * Column.java
 * ---------------
 * BasicTestCase: Volker Berlin
 * 
 */
package smallsql.junit;

import junit.framework.*;

import java.io.*;
import java.math.BigDecimal;
import java.sql.*;
import java.text.DateFormatSymbols;

public class BasicTestCase extends TestCase {

	/** Localized 3-letters months */
	protected static final String[] MONTHS = 
		new DateFormatSymbols().getShortMonths();

	public BasicTestCase(){
        super();
    }

    public BasicTestCase(String name){
        super(makeNameValid(name));
    }
    
    private static String makeNameValid(String name){
    	return name.replace(',' , ';').replace('(','{');
    }
    
    void dropTable(Connection con, String name) throws SQLException{
		try {
			Statement st = con.createStatement();
			st.execute("drop table "+name);
			st.close();
		} catch (SQLException e) {
            String msg = e.getMessage();
            if(msg.indexOf("[SmallSQL]Table")==0 && msg.indexOf(name)>0 && msg.indexOf("can't be dropped.")>0 ){
                return;
            }
            throw e;
        }
    }

    void dropView(Connection con, String name){
		try {
			Statement st = con.createStatement();
			st.execute("drop view "+name);
			st.close();
		} catch (SQLException e) {/* ignore it, if the view not exist */}
    }

	public void assertRSMetaData( ResultSet rs, String[] colNames, int[] types) throws Exception{
		ResultSetMetaData rm = rs.getMetaData();
		int count = rm.getColumnCount();
		assertEquals( "Column count:", colNames.length, count);
		for(int i=1; i<=count; i++){
			assertEquals("Col "+i+" name", colNames[i-1], rm.getColumnName(i));
			assertEquals("Col "+i+" label", colNames[i-1], rm.getColumnLabel(i));
			assertEquals("Col "+i+" type", types   [i-1], rm.getColumnType(i));
			switch(types[i-1]){
				case Types.VARCHAR:
					assertTrue  ("Wrong Precision (" + rm.getColumnTypeName(i) + ") for Column "+i+": "+rm.getPrecision(i), rm.getPrecision(i) > 0);
					break;
				case Types.INTEGER:
					assertTrue  ("Wrong Precision (" + rm.getColumnTypeName(i) + ") for Column "+i, rm.getPrecision(i) > 0);
					break;
			}
		}
	}
	
	private final static char[] digits = {'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};
	private static String bytes2hex( byte[] bytes ){
		StringBuffer buf = new StringBuffer(bytes.length << 1);
		for(int i=0; i<bytes.length; i++){
			buf.append( digits[ (bytes[i] >> 4) & 0x0F ] );
			buf.append( digits[ (bytes[i]     ) & 0x0F ] );
		}
		return buf.toString();
	}
	
	public void assertEqualsObject( String msg, Object obj1, Object obj2 ){
		if(obj1 instanceof byte[]){
			if(!java.util.Arrays.equals( (byte[])obj1, (byte[])obj2)){
				fail(msg + " expected:" + bytes2hex((byte[])obj1)+ " but was:"+bytes2hex((byte[])obj2));
			}
		}else{ 
			if(obj1 instanceof BigDecimal)
				if(((BigDecimal)obj1).compareTo((BigDecimal)obj2) == 0) return;
		
			assertEquals( msg, obj1, obj2);
		}
	}
	
    public void assertEqualsObject( String msg, Object obj1, Object obj2, boolean needTrim ){
        if(needTrim && obj1 != null){
            // trim for CHAR and BINARY
            if(obj1 instanceof String) obj1 = ((String)obj1).trim();
            if(obj1 instanceof byte[]){
                byte[] tmp = (byte[])obj1;
                int k=tmp.length-1;
                for(; k>= 0; k--) if(tmp[k] != 0) break;
                k++;
                byte[] tmp2 = new byte[k];
                System.arraycopy( tmp, 0, tmp2, 0, k);
                obj1 = tmp2;
            }
        }
		if(needTrim && obj2 != null){
			// trim for CHAR and BINARY
			if(obj2 instanceof String) obj2 = ((String)obj2).trim();
			if(obj2 instanceof byte[]){
				byte[] tmp = (byte[])obj2;
				int k=tmp.length-1;
				for(; k>= 0; k--) if(tmp[k] != 0) break;
				k++;
				byte[] tmp2 = new byte[k];
				System.arraycopy( tmp, 0, tmp2, 0, k);
				obj2 = tmp2;
			}
		}
		assertEqualsObject( msg, obj1, obj2);
    }
    
    
	void assertRowCount(int sollCount, String sql ) throws Exception{
		Connection con = AllTests.getConnection();
		Statement st = con.createStatement();
		ResultSet rs = st.executeQuery(sql);
        assertRowCount(sollCount,rs);
    }
    
    
    void assertRowCount(int sollCount, ResultSet rs ) throws Exception{
		int colCount = rs.getMetaData().getColumnCount();
		int count = 0;
		//System.out.println(sql);
		while(rs.next()){
			count++;
			for(int i=1; i<=colCount; i++){
				rs.getObject(i);
				//System.out.print( " "+rs.getObject(i));
			}
			//System.out.println();
		}
		assertEquals( "Wrong row count", sollCount, count);
		for(int i=1; i<=colCount; i++){
			try{
				// if not a SQLException occur then it is an error
				fail( "Column:"+i+" Value:"+String.valueOf(rs.getObject(i)));
			}catch(SQLException e){
                assertSQLException("01000", 0, e);
            }
		}
		assertFalse( "Scroll after last", rs.next() );
	}

	
    /**
     * Identical to the Implementation from Utils.string2boolean
     */
    private boolean string2boolean( String val){
        try{
            return Double.parseDouble( val ) != 0;
        }catch(NumberFormatException e){/*ignore it if it not a number*/}
        return "true".equalsIgnoreCase( val ) || "yes".equalsIgnoreCase( val ) || "t".equalsIgnoreCase( val );
    }
	
	/**
	 * Test a single Value of a the ResultSet that was produce from the SQL
	 */
   	void assertEqualsRsValue(Object obj, String sql) throws Exception{
		Connection con = AllTests.getConnection();
		Statement st = con.createStatement();
		ResultSet rs = st.executeQuery(sql);
		assertTrue( "No row produce", rs.next());
        assertEqualsRsValue(obj,rs,false);
    }
    
    
    void assertEqualsRsValue(Object obj, ResultSet rs, boolean needTrim) throws Exception{
        String name = rs.getMetaData().getColumnName(1);
		assertEqualsObject( "Values not identical on read:", obj, rs.getObject(name), needTrim);
		if(obj instanceof Time){
			assertEquals("Time is different:", obj, rs.getTime(name) );
			assertEquals("Time String is different:", obj.toString(), rs.getString(name) );
		}
		if(obj instanceof Timestamp){
			assertEquals("Timestamp is different:", obj, rs.getTimestamp(name) );
			assertEquals("Timestamp String is different:", obj.toString(), rs.getString(name) );
		}
		if(obj instanceof Date){
			assertEquals("Date is different:", obj, rs.getDate(name) );
			assertEquals("Date String is different:", obj.toString(), rs.getString(name) );
		}
		if(obj instanceof String){
            String str = (String)obj;
            assertEqualsObject("String is different:", str, rs.getString(name), needTrim );
			assertEquals("String Boolean is different:", string2boolean(str), rs.getBoolean(name) );
            try{
                assertEquals("String Long is different:", Long.parseLong(str), rs.getLong(name) );
            }catch(NumberFormatException ex){/* ignore */}
            try{
                assertEquals("String Integer is different:", Integer.parseInt(str), rs.getInt(name) );
            }catch(NumberFormatException ex){/* ignore */}
            try{
                assertEquals("String Float is different:", Float.parseFloat(str), rs.getFloat(name), 0.0 );
            }catch(NumberFormatException ex){/* ignore */}
            try{
                assertEquals("String Double is different:", Double.parseDouble(str), rs.getDouble(name), 0.0 );
            }catch(NumberFormatException ex){/* ignore */}
		}
		if(obj instanceof BigDecimal){
            if(!needTrim){
                assertEquals("BigDecimal is different:", obj, rs.getBigDecimal(name) );
                assertEquals("Scale is different:", ((BigDecimal)obj).scale(), rs.getMetaData().getScale(1));
            }
            assertEquals("Scale Meta is different:", rs.getBigDecimal(name).scale(), rs.getMetaData().getScale(1));
			BigDecimal big2 = ((BigDecimal)obj).setScale(2,BigDecimal.ROUND_HALF_EVEN);
			assertEquals("BigDecimal mit scale is different:", big2, rs.getBigDecimal(name, 2) );
		}
		if(obj instanceof Integer){
			assertEquals("Scale is different:", 0, rs.getMetaData().getScale(1));
		}
		if(obj instanceof Number){
            long longValue = ((Number)obj).longValue();
			int intValue = ((Number)obj).intValue();
            if(longValue >= Integer.MAX_VALUE)
                intValue = Integer.MAX_VALUE;
            if(longValue <= Integer.MIN_VALUE)
                intValue = Integer.MIN_VALUE;
			assertEquals("int is different:", intValue, rs.getInt(name) );
			assertEquals("long is different:", longValue, rs.getLong(name) );
			if(intValue >= Short.MIN_VALUE && intValue <= Short.MAX_VALUE)
				assertEquals("short is different:", (short)intValue, rs.getShort(name) );
			if(intValue >= Byte.MIN_VALUE && intValue <= Byte.MAX_VALUE)
				assertEquals("byte is different:", (byte)intValue, rs.getByte(name) );
			
			double value = ((Number)obj).doubleValue();
			assertEquals("Double is different:", value, rs.getDouble(name),0.0 );
			assertEquals("Float is different:", (float)value, rs.getFloat(name),0.0 );
			String valueStr = obj.toString();
            if(!needTrim){
                assertEquals("Number String is different:", valueStr, rs.getString(name) );
            }
			BigDecimal decimal = Double.isInfinite(value) || Double.isNaN(value) ? null : new BigDecimal(valueStr);
            assertEqualsObject("Number BigDecimal is different:", decimal, rs.getBigDecimal(name) );
			assertEquals("Number boolean is different:", value != 0, rs.getBoolean(name) );
		}
		if(obj == null){
			assertNull("String is different:", rs.getString(name) );
			assertNull("Date is different:", rs.getDate(name) );
			assertNull("Time is different:", rs.getTime(name) );
			assertNull("Timestamp is different:", rs.getTimestamp(name) );
			assertNull("BigDecimal is different:", rs.getBigDecimal(name) );
			assertNull("BigDecimal with scale is different:", rs.getBigDecimal(name, 2) );
			assertNull("Bytes with scale is different:", rs.getBytes(name) );
			assertEquals("Double is different:", 0, rs.getDouble(name),0 );
			assertEquals("Float is different:", 0, rs.getFloat(name),0 );
			assertEquals("Long is different:", 0, rs.getLong(name) );
			assertEquals("Int is different:", 0, rs.getInt(name) );
			assertEquals("SmallInt is different:", 0, rs.getShort(name) );
			assertEquals("TinyInt is different:", 0, rs.getByte(name) );
			assertEquals("Boolean is different:", false, rs.getBoolean(name) );
		}
		if(obj instanceof byte[]){
		    assertTrue("Binary should start with 0x", rs.getString(name).startsWith("0x"));
		}
		
		ResultSetMetaData metaData = rs.getMetaData();
		String className = metaData.getColumnClassName(1);
		assertNotNull( "ClassName:", className);
		if(obj != null){
			Class gotClass = Class.forName(className);
			Class objClass = obj.getClass();
			String objClassName = objClass.getName();
			
			int expectedLen = metaData.getColumnDisplaySize(1);

			// B/CLOBs must be treated as special cases			
			if (gotClass.equals(java.sql.Blob.class)) {
				assertTrue(
					"ClassName assignable: "+className+"<->"+objClassName,
					objClass.equals(new byte[0].getClass()));

				String message = "Check DisplaySize: " + expectedLen + "!=" + Integer.MAX_VALUE + ")";
				assertTrue( message, expectedLen == Integer.MAX_VALUE );
			}
			else if (gotClass.equals(java.sql.Clob.class)) { // same as NCLOB
				assertTrue(
					"ClassName assignable: "+className+"<->"+objClassName,
					objClass.equals(String.class));

				String message = "Check DisplaySize: " + expectedLen + "!=" + Integer.MAX_VALUE + ")";
				assertTrue( message, expectedLen == Integer.MAX_VALUE );
			}
			else {
				String foundStr = rs.getString(name);
				
				assertTrue("ClassName assignable: "+className+"<->"+objClassName, gotClass.isAssignableFrom(objClass));
				assertTrue( "DisplaySize to small "+ expectedLen +"<"+foundStr.length()+" (" + foundStr + ")", expectedLen >= foundStr.length() );
			}
		}
   	}
    
    
    void assertSQLException(String sqlstate, int vendorCode, SQLException ex) {
        StringWriter sw = new StringWriter();
        ex.printStackTrace(new PrintWriter(sw));
        assertEquals( "Vendor Errorcode:"+sw, vendorCode, ex.getErrorCode() );
        assertEquals( "SQL State:"+sw, sqlstate, ex.getSQLState());
    }
    

   	
	void printSQL(String sql) throws SQLException{
		Connection con = AllTests.getConnection();
		Statement st = con.createStatement();
		ResultSet rs = st.executeQuery(sql);
		printRS( rs );
	}
	
   	void printRS(ResultSet rs) throws SQLException{
   		int count = rs.getMetaData().getColumnCount();
		while(rs.next()){ 
			for(int i=1; i<=count; i++){
				System.out.print(rs.getString(i) + '\t');
			} 
			System.out.println();
		}

   	}
   	
   	/**
	 * Returns the localized 3-letters month.
	 * 
	 * @param ordinal
	 *            month ordinal (1-based).
	 * @return 3-letters month.
	 */
   	static String getMonth3L(int ordinal) {
   		return MONTHS[ordinal - 1];
   	}
}