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
 * ExpressionFunctionTimestampDiff.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 19.06.2004
 */
package smallsql.database;


/**
 * @author Volker Berlin
 */
public class ExpressionFunctionTimestampDiff extends ExpressionFunction {

	final private int interval;
	
	static final int mapIntervalType(int intervalType){
		switch(intervalType){
			case SQLTokenizer.MILLISECOND:
				return SQLTokenizer.SQL_TSI_FRAC_SECOND;
			case SQLTokenizer.SECOND:
				return SQLTokenizer.SQL_TSI_SECOND;
			case SQLTokenizer.MINUTE:
				return SQLTokenizer.SQL_TSI_MINUTE;
			case SQLTokenizer.HOUR:
				return SQLTokenizer.SQL_TSI_HOUR;
			case SQLTokenizer.D:
			case SQLTokenizer.DAY:
				return SQLTokenizer.SQL_TSI_DAY;
			case SQLTokenizer.WEEK:
				return SQLTokenizer.SQL_TSI_WEEK;
			case SQLTokenizer.MONTH:
				return SQLTokenizer.SQL_TSI_MONTH;
			case SQLTokenizer.QUARTER:
				return SQLTokenizer.SQL_TSI_QUARTER;
			case SQLTokenizer.YEAR:
				return SQLTokenizer.SQL_TSI_YEAR;
			default:
				return intervalType;
		}
	}
	
	ExpressionFunctionTimestampDiff(int intervalType, Expression p1, Expression p2){
		interval = mapIntervalType( intervalType );
		setParams( new Expression[]{p1,p2});
	}
	
	int getFunction() {
		return SQLTokenizer.TIMESTAMPDIFF;
	}


	boolean isNull() throws Exception {
		return param1.isNull() || param2.isNull();
	}


	boolean getBoolean() throws Exception {
		return getInt() != 0;
	}


	int getInt() throws Exception {
		if(isNull()) return 0;
		switch(interval){
			case SQLTokenizer.SQL_TSI_FRAC_SECOND:
				return (int)(param2.getLong() - param1.getLong());
			case SQLTokenizer.SQL_TSI_SECOND:
				return (int)(param2.getLong() /1000 - param1.getLong() /1000);
			case SQLTokenizer.SQL_TSI_MINUTE:
				return (int)(param2.getLong() /60000 - param1.getLong() /60000);
			case SQLTokenizer.SQL_TSI_HOUR:
				return (int)(param2.getLong() /3600000 - param1.getLong() /3600000);
			case SQLTokenizer.SQL_TSI_DAY:
				return (int)(param2.getLong() /86400000 - param1.getLong() /86400000);
			case SQLTokenizer.SQL_TSI_WEEK:{
				long day2 = param2.getLong() /86400000;
				long day1 = param1.getLong() /86400000;
				// the 1. Jan 1970 is a Thursday --> 3
				return (int)((day2 + 3) / 7 - (day1 + 3) / 7);
			}case SQLTokenizer.SQL_TSI_MONTH:{
				DateTime.Details details2 = new DateTime.Details(param2.getLong());
				DateTime.Details details1 = new DateTime.Details(param1.getLong());
				return (details2.year * 12 + details2.month) - (details1.year * 12 + details1.month);
			}
			case SQLTokenizer.SQL_TSI_QUARTER:{
				DateTime.Details details2 = new DateTime.Details(param2.getLong());
				DateTime.Details details1 = new DateTime.Details(param1.getLong());
				return (details2.year * 4 + details2.month / 3) - (details1.year * 4 + details1.month / 3);
			}
			case SQLTokenizer.SQL_TSI_YEAR:{
				DateTime.Details details2 = new DateTime.Details(param2.getLong());
				DateTime.Details details1 = new DateTime.Details(param1.getLong());
				return details2.year - details1.year;
			}
			default: throw new Error();
		}
	}


	long getLong() throws Exception {
		return getInt();
	}


	float getFloat() throws Exception {
		return getInt();
	}


	double getDouble() throws Exception {
		return getInt();
	}


	long getMoney() throws Exception {
		return getInt() * 10000L;
	}


	MutableNumeric getNumeric() throws Exception {
		if(isNull()) return null;
		return new MutableNumeric(getInt());
	}


	Object getObject() throws Exception {
		if(isNull()) return null;
		return Utils.getInteger(getInt());
	}


	String getString() throws Exception {
		if(isNull()) return null;
		return String.valueOf(getInt());
	}


	int getDataType() {
		return SQLTokenizer.INT;
	}

}
