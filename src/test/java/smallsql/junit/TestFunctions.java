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
 * TestFunktions.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.junit;

import junit.framework.*;

import java.math.*;
import java.sql.*;

public class TestFunctions extends BasicTestCase{

    private TestValue testValue;

    private static final String table = "table_functions";

    private static final TestValue[] TESTS = new TestValue[]{
		a("$3"               	, new BigDecimal("3.0000")),
	    a("$-3.1"              	, new BigDecimal("-3.1000")),
	    a("-$3.2"              	, new BigDecimal("-3.2000")),
	    a("1 + 2"               , new Integer(3)),
        a("3 * 2"               , new Integer(6)),
        a("Top 1 4 / 2"         , new Integer(2)),
        a("7/3"         		, new Integer(2)),
        a("5 - 2"               , new Integer(3)),
        a("- aint"              , new Integer(120)),
        a("5 - - 2"             , new Integer(7)),
        a("5 - - - 2"           , new Integer(3)),
		a("-.123E-1"            , new Double("-0.0123")),
		a(".123E-1"             , new Double("0.0123")),
		a("123e-1"              , new Double("12.3")),
		a("123E1"               , new Double("1230")),
		a("2*5+2"               , new Integer("12")),
		a("'a''b'"              , "a'b"),
		a("'a\"b'"              , "a\"b"),
        a("~1"                  , new Integer(-2)),
        a("abs(-5)"             , new Integer(5)),
        a("abs(aint)"           , new Integer(120)),
        a("abs("+table+".aint)" , new Integer(120)),
        a("abs(null)"           , null),
        a("abs(cast(5 as money))"  , new BigDecimal("5.0000")),
        a("abs(cast(-5 as money))" , new BigDecimal("5.0000")),
        a("abs(cast(-5 as numeric(4,2)))" , new BigDecimal("5.00")),
        a("abs(cast(5 as real))"   , new Float(5)),
        a("abs(cast(-5 as real))"  , new Float(5)),
        a("abs(cast(-5 as float))" , new Double(5)),
        a("abs(cast(5 as double))" , new Double(5)),
        a("abs(cast(5 as smallint))",new Integer(5)),
        a("abs(cast(-5 as bigint))", new Long(5)),
        a("abs(cast(5 as bigint))",  new Long(5)),
        a("convert(money, abs(-5))", new BigDecimal("5.0000")),
		a("convert(varchar(30), 11)" 	, "11"),
		a("convert(varchar(30), null)" 	, null),
		a("convert(varchar(1), 12)" 	, "1"),
		a("convert(char(5), 11)" 		, "11   "),
		a("convert(longvarchar, {d '1999-10-12'})" 	, "1999-10-12"),
		a("convert(binary(5), '11')" 	, new byte[]{'1','1',0,0,0}),
		a("convert(binary(5), null)" 	, null),
		a("convert(varbinary(5), 11)" 	, new byte[]{0,0,0,11}),
		a("convert(longvarbinary, '11')", new byte[]{'1','1'}),
		a("convert(varchar(30),convert(varbinary(30),'Meherban'))", "Meherban"),
		a("convert(bit, 1)" 			, Boolean.TRUE),
		a("convert(bit, false)" 		, Boolean.FALSE),
		a("convert(boolean, 0)" 		, Boolean.FALSE),
		a("convert(varchar(30), convert(bit, false))" 		, "0"),
		a("convert(varchar(30), convert(boolean, 0))" 		, "false"),
		a("convert(bigint, 11)" 		, new Long(11)),
		a("convert(int, 11)" 			, new Integer(11)),
		a("{fn convert(11, Sql_integer)}" 			, new Integer(11)),
		a("convert(integer, 11)" 			, new Integer(11)),
		a("convert(smallint, 123456789)", new Integer((short)123456789)),
		a("convert(tinyint, 123456789)"	, new Integer(123456789 & 0xFF)),
		a("convert(date, '1909-10-12')" , Date.valueOf("1909-10-12")),
		a("convert(date, null)" 		, null),
		a("convert(date, {ts '1999-10-12 15:14:13.123456'})" 	, Date.valueOf("1999-10-12")),
		a("convert(date, now())" 		, Date.valueOf( new Date(System.currentTimeMillis()).toString()) ),
		a("curdate()" 					, Date.valueOf( new Date(System.currentTimeMillis()).toString()) ),
		a("current_date()" 				, Date.valueOf( new Date(System.currentTimeMillis()).toString()) ),
		a("hour(curtime())" 			, new Integer(new Time(System.currentTimeMillis()).getHours()) ),
		a("minute({t '10:11:12'})" 		, new Integer(11) ),
		a("month( {ts '1899-10-12 15:14:13.123456'})" 	, new Integer(10)),
		a("year({d '2004-12-31'})"    , new Integer(2004)),
		a("convert(time, '15:14:13')" 	, Time.valueOf("15:14:13")),
		a("convert(time, null)" 		, null),
		a("convert(timestamp, '1999-10-12 15:14:13.123456')" 	, Timestamp.valueOf("1999-10-12 15:14:13.123")),
        a("cast({ts '1907-06-05 04:03:02.1'} as smalldatetime)", Timestamp.valueOf("1907-06-05 04:03:00.0")),
        a("cast({ts '2007-06-05 04:03:02.1'} as smalldatetime)", Timestamp.valueOf("2007-06-05 04:03:00.0")),
		a("convert(varchar(30), {d '1399-10-12 3:14:13'},  -1)" 	, "1399-10-12"),
		a("convert(varchar(30), {ts '1999-10-12  3:14:13.12'},  99)" 	, "1999-10-12 03:14:13.12"),
		a("convert(varchar(30), {ts '1999-10-12  0:14:13.123456'},   0)" 	, getMonth3L(10) + " 12 1999 12:14AM"),
		a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'},   1)" 	, "10/12/99"),
		a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'},   2)" 	, "99.10.12"),
		a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'},   3)" 	, "12/10/99"),
		a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'},   4)" 	, "12.10.99"),
		a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'},   5)" 	, "12-10-99"),
		a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'},   6)" 	, "12 " + getMonth3L(10) + " 99"),
		a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'},   7)" 	, getMonth3L(10) + " 12, 99"),
		a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'},   8)" 	, "15:14:13"),
		a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'},   9)" 	, getMonth3L(10) + " 12 1999 03:14:13:123PM"),
		a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'},  10)" 	, "10-12-99"),
		a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'},  11)" 	, "99/10/12"),
		a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'},  12)" 	, "991012"),
		a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'},  13)" 	, "12 " + getMonth3L(10) + " 1999 15:14:13:123"),
		a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'},  14)" 	, "15:14:13:123"),
		a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'},  20)" 	, "1999-10-12 15:14:13"),
		a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'},  21)" 	, "1999-10-12 15:14:13.123"),
		a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'}, 100)" 	, getMonth3L(10) + " 12 1999 03:14PM"),
		a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'}, 101)" 	, "10/12/1999"),
		a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'}, 102)" 	, "1999.10.12"),
		a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'}, 103)" 	, "12/10/1999"),
		a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'}, 104)" 	, "12.10.1999"),
		a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'}, 105)" 	, "12-10-1999"),
		a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'}, 106)" 	, "12 " + getMonth3L(10) + " 1999"),
		a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'}, 107)" 	, getMonth3L(10) + " 12, 1999"),
		a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'}, 108)" 	, "15:14:13"),
		a("convert(varchar(30), {ts '1999-10-12  3:14:13.123456'}, 109)" 	, getMonth3L(10) + " 12 1999 03:14:13:123AM"),
		a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'}, 109)" 	, getMonth3L(10) + " 12 1999 03:14:13:123PM"),
		a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'}, 110)" 	, "10-12-1999"),
		a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'}, 111)" 	, "1999/10/12"),
		a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'}, 112)" 	, "19991012"),
		a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'}, 113)" 	, "12 " + getMonth3L(10) + " 1999 15:14:13:123"),
		a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'}, 114)" 	, "15:14:13:123"),
		a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'}, 120)" 	, "1999-10-12 15:14:13"),
		a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'}, 121)" 	, "1999-10-12 15:14:13.123"),
		a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'}, 126)" 	, "1999-10-12T15:14:13.123"),
		a("convert(varchar(30), {ts '1999-10-12  3:14:13.123456'}, 130)" 	, "12 " + getMonth3L(10) + " 1999 03:14:13:123AM"),
		a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'}, 130)" 	, "12 " + getMonth3L(10) + " 1999 03:14:13:123PM"),
		a("convert(varchar(30), {ts '1999-10-12 15:14:13.123456'}, 131)" 	, "12/10/99 15:14:13:123"),
		a("convert(timestamp, null)" 	, null),
		a("convert(real, 11)" 			, new Float(11)),
		a("convert(real, null)" 		, null),
		a("convert(float, 11.0)" 		, new Double(11)),
		a("convert(double, '11')" 		, new Double(11)),
		a("-convert(decimal, '11.123456')" 		, new BigDecimal("-11")),
		a("-convert(decimal(38,6), '11.123456')" 		, new BigDecimal("-11.123456")),
		a("convert(decimal(38,6), '11.123456') + 1" 		, new BigDecimal("12.123456")),
		a("convert(decimal(38,6), '11.123456') - 1" 		, new BigDecimal("10.123456")),
		a("convert(decimal(12,2), '11.0000') * 1" 		, new BigDecimal("11.00")),
		a("convert(decimal(12,2), '11.0000') * convert(decimal(12,2), 1)" 		, new BigDecimal("11.0000")),
		a("convert(decimal(12,2), '11.0000') / 1" 		, new BigDecimal("11.0000000")), //scale = Max(left scale+5, right scale +4)
		a("convert(decimal(12,0), 11) / convert(decimal(12,2), 1)" 		, new BigDecimal("11.000000")), //scale = Max(left scale+5, right scale +4)
		a("convert(money, -10000 / 10000.0)" 		, new BigDecimal("-1.0000")), //scale = Max(left scale+5, right scale +4)
		a("-convert(money, '11.123456')" 		, new BigDecimal("-11.1235")),
		a("-convert(smallmoney, '11.123456')" 	, new BigDecimal("-11.1235")),
		a("convert(uniqueidentifier, 0x12345678901234567890)" 	, "78563412-1290-5634-7890-000000000000"),
		a("convert(uniqueidentifier, '78563412-1290-5634-7890-000000000000')" 	, "78563412-1290-5634-7890-000000000000"),
		a("convert(binary(16), convert(uniqueidentifier, 0x12345678901234567890))" 	, new byte[]{0x12,0x34,0x56,0x78,(byte)0x90,0x12,0x34,0x56,0x78,(byte)0x90,0,0,0,0,0,0}),
		a("Timestampdiff(day,         {d '2004-10-12'}, {d '2004-10-14'})" 		, new Integer(2)),
		a("Timestampdiff(SQL_TSI_DAY, {d '2004-10-12'}, {d '2004-10-15'})" 		, new Integer(3)),
		a("Timestampdiff(d,           {d '2004-10-12'}, {d '2004-10-16'})" 		, new Integer(4)),
		a("Timestampdiff(dd,          {d '2004-10-12'}, {d '2004-10-17'})" 		, new Integer(5)),
		a("Timestampdiff(SQL_TSI_YEAR,{d '2000-10-12'}, {d '2005-10-17'})" 		, new Integer(5)),
		a("Timestampdiff(year,			{d '2000-10-12'}, {d '2005-10-17'})" 		, new Integer(5)),
		a("Timestampdiff(SQL_TSI_QUARTER,{d '2000-10-12'}, {d '2005-10-17'})" 	, new Integer(20)),
		a("Timestampdiff(quarter,		{d '2000-10-12'}, {d '2005-10-17'})" 	, new Integer(20)),
		a("Timestampdiff(SQL_TSI_MONTH,	{d '2004-10-12'}, {d '2005-11-17'})" 	, new Integer(13)),
		a("Timestampdiff(month,			{d '2004-10-12'}, {d '2005-11-17'})" 	, new Integer(13)),
		a("Timestampdiff(SQL_TSI_WEEK,	{d '2004-10-09'}, {d '2004-10-12'})" 		, new Integer(1)),
		a("Timestampdiff(week,			{d '2004-10-09'}, {d '2004-10-12'})" 		, new Integer(1)),
		a("Timestampdiff(SQL_TSI_HOUR,	{d '2004-10-12'}, {d '2004-10-13'})" 		, new Integer(24)),
		a("Timestampdiff(hour,			{d '2004-10-12'}, {d '2004-10-13'})" 		, new Integer(24)),
		a("Timestampdiff(SQL_TSI_MINUTE,{t '10:10:10'}, {t '11:11:11'})" 		, new Integer(61)),
		a("Timestampdiff(minute,		{t '10:10:10'}, {t '11:11:11'})" 		, new Integer(61)),
		a("Timestampdiff(SQL_TSI_SECOND,{t '00:00:10'}, {t '00:10:11'})" 		, new Integer(601)),
		a("Timestampdiff(second,		{t '00:00:10'}, {t '00:10:11'})" 		, new Integer(601)),
		a("Timestampdiff(SQL_TSI_FRAC_SECOND,{ts '2004-10-12 00:00:10.1'}, {ts '2004-10-12 00:00:10.2'})" 		, new Integer(100)),
		a("Timestampdiff(millisecond,{ts '2004-10-12 00:00:10.1'}, {ts '2004-10-12 00:00:10.2'})" 		, new Integer(100)),
		a("{fn TimestampAdd(SQL_TSI_YEAR,     1, {d '2004-10-17'})}" 		, Timestamp.valueOf("2005-10-17 00:00:00.0")),
        a("{fn TimestampAdd(SQL_TSI_QUARTER,  1, {d '2004-10-17'})}"        , Timestamp.valueOf("2005-01-17 00:00:00.0")),
        a("{fn TimestampAdd(SQL_TSI_MONTH,    1, {d '2004-10-17'})}"        , Timestamp.valueOf("2004-11-17 00:00:00.0")),
        a("{fn TimestampAdd(SQL_TSI_WEEK,     1, {d '2004-10-17'})}"        , Timestamp.valueOf("2004-10-24 00:00:00.0")),
        a("{fn TimestampAdd(SQL_TSI_HOUR,     1, {d '2004-10-17'})}"        , Timestamp.valueOf("2004-10-17 01:00:00.0")),
        a("{fn TimestampAdd(SQL_TSI_MINUTE,  61, {d '2004-10-17'})}"        , Timestamp.valueOf("2004-10-17 01:01:00.0")),
        a("{fn TimestampAdd(SQL_TSI_SECOND,  61, {d '2004-10-17'})}"        , Timestamp.valueOf("2004-10-17 00:01:01.0")),
        a("{fn TimestampAdd(SQL_TSI_FRAC_SECOND,1,{d '2004-10-17'})}"       , Timestamp.valueOf("2004-10-17 00:00:00.001")),
		a("Timestampdiff(second, null, {t '00:10:11'})" 		, null),
		a("Timestampdiff(second, {t '00:10:11'}, null)" 		, null),
		a("TimestampAdd(year,     1, null)" 		, null),
		a("DayOfWeek({d '2006-02-16'})" 		, new Integer(4)),
		a("DayOfWeek({d '2006-02-19'})" 		, new Integer(7)),
		a("DayOfYear({d '2004-01-01'})" 		, new Integer(1)),
		a("DayOfYear({d '2004-02-29'})" 		, new Integer(60)),
		a("DayOfYear({d '2004-03-01'})" 		, new Integer(61)),
		a("DayOfYear({d '2004-12-31'})" 		, new Integer(366)),
		a("DayOfMonth({d '1904-07-17'})" 		, new Integer(17)),
		a("locate('ae', 'QWAERAE')"		, new Integer(3)),
		a("locate('ae', 'QWAERAE', 3)"	, new Integer(3)),
		a("locate('ae', 'QWAERAE', 4)"	, new Integer(6)),
		a("locate('ae', 'QWAERAE', null)"		, new Integer(3)),
		a("locate(null, 'QWAERAE', 4)"	, null),
		a("locate('ae', null, 4)"	, null),
		a("{d '2004-10-12'}"	, 				java.sql.Date.valueOf("2004-10-12")),
		a("{ts '1999-10-12 15:14:13.123'}"	, 	Timestamp.valueOf("1999-10-12 15:14:13.123")),
		a("{t '15:14:13'}"	, 					Time.valueOf("15:14:13")),
		a("{fn length('abc')}", 				new Integer(3)),
		a("{fn length('abc ')}", 				new Integer(3)),
		a("{fn length(null)}", 					null),
		a("{fn Right('qwertzu', 3)}", 			"tzu"),
		a("{fn Right('qwertzu', 13)}", 			"qwertzu"),
		a("cast( Right('1234', 2) as real)", 	new Float(34)),
		a("cast( Right('1234', 2) as smallint)",new Integer(34)),
		a("cast( Right('1234', 2) as boolean)", Boolean.TRUE),
		a("right(0x1234567890, 2)",				new byte[]{0x78,(byte)0x90}),
		a("right(null, 2)",						null),
        a("left(null, 2)",                      null),
        a("left('abcd', 2)",                    "ab"),
        a("left(0x1234567890, 2)",              new byte[]{0x12,(byte)0x34}),
		a("cast({fn SubString('ab2.3qw', 3, 3)} as double)", 	new Double(2.3)),
		a("subString('qwert', 99, 2)", 		""),
		a("{fn SubString(0x1234567890, 0, 99)}",new byte[]{0x12,0x34,0x56,0x78,(byte)0x90}),
		a("{fn SubString(0x1234567890, 2, 2)}", new byte[]{0x34, 0x56}),
		a("{fn SubString(0x1234567890, 99, 2)}", new byte[]{}),
		a("SubString(null, 99, 2)", 			null),
        a("Insert('abcd', 2, 1, 'qw')",         "aqwcd"),
        a("Insert(0x1234, 2, 0, 0x56)",         new byte[]{0x12,0x56,0x34}),
        a("STUFF(null, 2, 0, 0x56)",         	null),
        a("lcase('Abcd')",                      "abcd"),
        a("ucase('Abcd')",                      "ABCD"),
        a("lcase(null)",                        null),
        a("ucase(null)",                        null),
        a("cast(1 as money) + SubString('a12', 2, 2)",new BigDecimal("13.0000")),
        a("cast(1 as numeric(5,2)) + SubString('a12', 2, 2)",new BigDecimal("13.00")),
        a("cast(1 as BigInt) + SubString('a12', 2, 2)",new Long(13)),
        a("cast(1 as real) + SubString('a12', 2, 2)",new Float(13)),
        a("1   + SubString('a12', 2, 2)",       new Integer(13)),
        a("1.0 + SubString('a12', 2, 2)",       new Double(13)),
        a("concat('abc', 'def')",               "abcdef"),
		a("{fn IfNull(null, 'abc')}", 			"abc"),
		a("{fn IfNull('asd', 'abc')}", 			"asd"),
		a("iif(true, 1, 2)", 					new Integer(1)),
		a("iif(false, 1, 2)", 					new Integer(2)),
		a("CASE aVarchar WHEN 'qwert' THEN 25 WHEN 'asdfg' THEN 26 ELSE null END", new Integer(25)),
		a("CASE WHEN aVarchar='qwert' THEN 'uu' WHEN aVarchar='bb' THEN 'gg' ELSE 'nn' END", "uu"),
		a("{fn Ascii('')}", 			null),
		a("{fn Ascii(null)}", 			null),
		a("Ascii('abc')", 				new Integer(97)),
		a("{fn Char(97)}", 				"a"),
		a("Char(null)", 				null),
        a("$1 + Char(49)",              new BigDecimal("2.0000")),
		a("Exp(null)", 					null),
		a("exp(0)", 					new Double(1)),
		a("log(exp(2.4))", 				new Double(2.4)),
		a("log10(10)", 					new Double(1)),
		a("cos(null)", 					null),
		a("cos(0)", 					new Double(1)),
		a("acos(1)", 					new Double(0)),
		a("sin(0)", 					new Double(0)),
		a("cos(pi())", 					new Double(-1)),
		a("asin(0)", 					new Double(0)),
		a("asin(sin(0.5))",				new Double(0.5)),
		a("tan(0)", 					new Double(0)),
		a("atan(tan(0.5))",				new Double(0.5)),
		a("atan2(0,3)",					new Double(0)),
		a("atan2(0,-3)",				new Double(Math.PI)),
		a("atn2(0,null)",				null),
		a("cot(0)",						new Double(Double.POSITIVE_INFINITY)),
		a("tan(0)", 					new Double(0)),
		a("degrees(pi())", 				new Double(180)),
		a("degrees(radians(50))", 		new Double(50)),
		a("ceiling(123.45)", 			new Double(124)),
		a("ceiling(-123.45)", 			new Double(-123)),
		a("power(2, 3)", 				new Double(8)),
		a("5.0 % 2", 					new Double(1)),
		a("5 % 2", 						new Integer(1)),
		a("mod(5, 2)", 					new Integer(1)),
		a("FLOOR(123.45)", 				new Double(123)),
		a("FLOOR('123.45')", 			new Double(123)),
		a("FLOOR(-123.45)", 			new Double(-124)),
		a("FLOOR($123.45)", 			new BigDecimal("123.0000")),
		a("Rand(0)", 					new Double(0.730967787376657)),
		a("ROUND(748.58, -4)", 			new Double(0)),
		a("ROUND(-748.58, -2)", 		new Double(-700)),
		a("ROUND('748.5876', 2)", 		new Double(748.59)),
        a("round( 1e19, 0)"       , new Double(1e19)),
        a("truncate( -1e19,0)"      , new Double(-1e19)),
		a("Sign('748.5876')", 			new Integer(1)),
		a("Sign(-2)", 					new Integer(-1)),
        a("Sign(2)",                    new Integer(1)),
        a("Sign(0)",                    new Integer(0)),
        a("Sign(-$2)",                  new Integer(-1)),
        a("Sign($2)",                   new Integer(1)),
        a("Sign($0)",                   new Integer(0)),
        a("Sign(cast(-2 as bigint))",   new Integer(-1)),
        a("Sign(cast(2 as bigint))",    new Integer(1)),
        a("Sign(cast(0 as bigint))",    new Integer(0)),
        a("Sign(1.0)",                  new Integer(1)),
		a("Sign(0.0)", 					new Integer(0)),
        a("Sign(-.1)",                  new Integer(-1)),
        a("Sign(cast(0 as numeric(5)))",new Integer(0)),
		a("Sign(null)", 				null),
		a("sqrt(9)", 					new Double(3)),
		a("Truncate(748.58, -4)", 		new Double(0)),
		a("Truncate(-748.58, -2)", 		new Double(-700)),
		a("Truncate('748.5876', 2)", 	new Double(748.58)),
        a("rtrim(null)",                null),
        a("rtrim(0x0012345600)",        new byte[]{0x00,0x12,0x34,0x56}),
        a("rtrim(' abc ')",             " abc"),
        a("ltrim(null)",                null),
        a("ltrim(0x0012345600)",        new byte[]{0x12,0x34,0x56,0x00}),
        a("ltrim(' abc ')",             "abc "),
        a("space(3)",                   "   "),
        a("space(null)",                null),
        a("space(-3)",                  null),
        a("replace('abcabc','bc','4')", "a4a4"),
        a("replace('abcabc','bc',null)",null),
        a("replace('abcabc','','4')",   "abcabc"),
        a("replace(0x123456,0x3456,0x77)", new byte[]{0x12,0x77}),
        a("replace(0x123456,0x,0x77)",  new byte[]{0x12,0x34,0x56}),
        a("replace(0x123456,0x88,0x77)",new byte[]{0x12,0x34,0x56}),
        a("repeat('ab',4)",             "abababab"),
        a("repeat(null,4)",             null),
        a("repeat(0x1234,3)",           new byte[]{0x12,0x34,0x12,0x34,0x12,0x34}),
        a("DIFFERENCE('Green','Greene')",new Integer(4)),
        a("DIFFERENCE('Green',null)",   null),
        a("OCTET_LENGTH('SomeWord')",   new Integer(16)),
        a("OCTET_LENGTH('')",   		new Integer(0)),
        a("OCTET_LENGTH(null)",   		null),
        a("BIT_LENGTH('SomeWord')",     new Integer(128)),
        a("BIT_LENGTH('')",   		    new Integer(0)),
        a("BIT_LENGTH(null)",   		null),
        a("CHAR_LENGTH('SomeWord')",    new Integer(8)),
        a("CHAR_LENGTH('')",   		    new Integer(0)),
        a("CHAR_LENGTH(null)",   		null),
        a("CHARACTER_LENGTH('SomeWord')", new Integer(8)),
        a("CHARACTER_LENGTH('')",   	new Integer(0)),
        a("CHARACTER_LENGTH(null)",   	null),
        a("soundex('Wikipedia')",       "W213"),
        a("0x10 < 0x1020",              Boolean.TRUE),
	};


    private static TestValue a(String function, Object result){
        TestValue value = new TestValue();
        value.function  = function;
        value.result    = result;
        return value;
    }

    TestFunctions(TestValue testValue){
        super(testValue.function);
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
            st.execute("create table " + table + "(aInt int, aVarchar varchar(100))");
            st.execute("Insert into " + table + "(aInt, aVarchar) Values(-120,'qwert')");
            st.close();
        }catch(Throwable e){
            e.printStackTrace();
        }
    }

    public void runTest() throws Exception{
    	String query = "Select " + testValue.function + ",5 from " + table;
		assertEqualsRsValue( testValue.result, query);
        if(!testValue.function.startsWith("Top")){
            assertEqualsRsValue( testValue.result, "Select " + testValue.function + " from " + table + " Group By " + testValue.function);
        }
    }

    public static Test suite() throws Exception{
        TestSuite theSuite = new TestSuite("Functions");
        for(int i=0; i<TESTS.length; i++){
            theSuite.addTest(new TestFunctions( TESTS[i] ) );
        }
        return theSuite;
    }

    private static class TestValue{
        String function;
        Object result;
    }
}