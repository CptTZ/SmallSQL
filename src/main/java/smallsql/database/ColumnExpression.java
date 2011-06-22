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
 * ColumnExpression.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 13.06.2004
 */
package smallsql.database;


/**
 * A Column how it is used for Views. It based on a Expression and not on a native Column from a Table.
 * This Column is read only.
 * 
 * @author Volker Berlin
 */
class ColumnExpression extends Column {

	final private Expression expr;
	
	ColumnExpression(Expression expr){
		this.expr = expr;
	}
	
	
	String getName(){
		return expr.getAlias();
	}

	
	boolean isAutoIncrement(){
		return expr.isAutoIncrement();
	}

	
	boolean isCaseSensitive(){
		return expr.isCaseSensitive();
	}


	boolean isNullable(){
		return expr.isNullable();
	}


	int getDataType(){
		return expr.getDataType();
	}


	int getDisplaySize(){
		return expr.getDisplaySize();
	}


	int getScale(){
		return expr.getScale();
	}


	int getPrecision(){
		return expr.getPrecision();
	}

}
