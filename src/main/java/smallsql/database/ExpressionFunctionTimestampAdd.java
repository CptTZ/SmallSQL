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
 * ExpressionFunctionTimestampAdd.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 19.06.2004
 */
package smallsql.database;


/**
 * @author Volker Berlin
 */
public class ExpressionFunctionTimestampAdd extends ExpressionFunction {

	final private int interval;
	

	ExpressionFunctionTimestampAdd(int intervalType, Expression p1, Expression p2){
		interval = ExpressionFunctionTimestampDiff.mapIntervalType( intervalType );
		setParams( new Expression[]{p1,p2});
	}
	
	
	int getFunction() {
		return SQLTokenizer.TIMESTAMPADD;
	}


	boolean isNull() throws Exception {
		return param1.isNull() || param2.isNull();
	}


	boolean getBoolean() throws Exception {
		return getLong() != 0;
	}


	int getInt() throws Exception {
		return (int)getLong();
	}


	long getLong() throws Exception {
		if(isNull()) return 0;
		switch(interval){
			case SQLTokenizer.SQL_TSI_FRAC_SECOND:
				return param2.getLong() + param1.getLong();
			case SQLTokenizer.SQL_TSI_SECOND:
				return param2.getLong() + param1.getLong() * 1000;
			case SQLTokenizer.SQL_TSI_MINUTE:
				return param2.getLong() + param1.getLong() * 60000;
			case SQLTokenizer.SQL_TSI_HOUR:
				return param2.getLong() + param1.getLong() * 3600000;
			case SQLTokenizer.SQL_TSI_DAY:
				return param2.getLong() + param1.getLong() * 86400000;
			case SQLTokenizer.SQL_TSI_WEEK:{
				return param2.getLong() + param1.getLong() * 604800000;
			}case SQLTokenizer.SQL_TSI_MONTH:{
				DateTime.Details details2 = new DateTime.Details(param2.getLong());
				details2.month += param1.getLong();
				return DateTime.calcMillis(details2);
			}
			case SQLTokenizer.SQL_TSI_QUARTER:{
				DateTime.Details details2 = new DateTime.Details(param2.getLong());
				details2.month += param1.getLong() * 3;
				return DateTime.calcMillis(details2);
			}
			case SQLTokenizer.SQL_TSI_YEAR:{
				DateTime.Details details2 = new DateTime.Details(param2.getLong());
				details2.year += param1.getLong();
				return DateTime.calcMillis(details2);
			}
			default: throw new Error();
		}
		// TODO Auto-generated method stub
	}


	float getFloat() throws Exception {
		return getLong();
	}


	double getDouble() throws Exception {
		return getLong();
	}
	

	long getMoney() throws Exception {
		return getLong() * 10000;
	}


	MutableNumeric getNumeric() throws Exception {
		if(isNull()) return null;
		return new MutableNumeric(getLong());
	}


	Object getObject() throws Exception {
		if(isNull()) return null;
		return new DateTime( getLong(), SQLTokenizer.TIMESTAMP );
	}


	String getString() throws Exception {
		if(isNull()) return null;
		return new DateTime( getLong(), SQLTokenizer.TIMESTAMP ).toString();
	}


	int getDataType() {
		return SQLTokenizer.TIMESTAMP;
	}

}
