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
 * SmallSQLException.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.database;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.MessageFormat;

import smallsql.database.language.Language;

/**
 * @author Volker Berlin
 *
 */
class SmallSQLException extends SQLException {
    private static final long serialVersionUID = -1683756623665114L;
    private boolean isInit;
    private static Language language;

	/**
	 * Creates an exception with the specified message.
	 * 
	 * @param message
	 *            instantiated exception message.
	 */
	private SmallSQLException(String message, String vendorCode) {
		super("[SmallSQL]" + message, vendorCode, 0);
		init();
	}
	/**
	 * Creates an exception with the specified message, appending the passed
	 * one.
	 * 
	 * @param throwable
	 *            exception to append.
	 * @param message
	 *            instantiated exception message.
	 */
	private SmallSQLException(Throwable throwable, String message, String vendorCode) {
		super("[SmallSQL]" + message, vendorCode, 0);
		this.initCause(throwable);
		init();
	}
	
	private void init(){
		this.isInit = true;
		PrintWriter pw = DriverManager.getLogWriter();
		if(pw != null) this.printStackTrace(pw);	
	}
	
    /**
	 * Sets the language for the specified locale.<br>
	 * For information, see the specific document.
	 * 
	 * @param localeObj
	 *            nullable; can be a Locale object, or its Locale.toString()
	 *            String.
	 * @throws SQLException
	 *             in case of missing language resource.
	 */
    static void setLanguage(Object localeObj) throws SQLException {
    	// if already set and no lang passed, return!
    	if (language != null && localeObj == null) return;

    	if (localeObj == null) {
    		language = Language.getDefaultLanguage(); 
    	}
    	else {
    		language = Language.getLanguage(localeObj.toString()); 
    	}
    }
    
	public void printStackTrace(){
		if(!isInit) return;
		super.printStackTrace();
	}
    
	public void printStackTrace(PrintStream ps){
		if(!isInit) return;
		super.printStackTrace(ps);
	}
    
	public void printStackTrace(PrintWriter pw){
		if(!isInit) return;
		super.printStackTrace(pw);
	}
	
	//////////////////////////////////////////////////////////////////////
	// FACTORY METHODS
	//////////////////////////////////////////////////////////////////////
	
    static SQLException create( String messageCode ) {
    	assert (messageCode != null): "Fill parameters";
    	
    	String message = translateMsg(messageCode, null);
    	String sqlState = language.getSqlState(messageCode);
        return new SmallSQLException(message, sqlState);
    }
    
    /**
	 * Convenience method for passing only one parameter.<br>
	 * To create a custom message, pass Language.CUSTOM_MESSAGE as messageCode
	 * and the message as param0.
	 * 
	 * @param messageCode
	 *            localized message key. pass Language.CUSTOM_MESSAGE and the
	 *            plain message inside the parameters array to create an
	 *            unlocalized message.
	 * @param param0
	 *            message parameter.
	 */
    static SQLException create( String messageCode, Object param0 ) {
    	String message = translateMsg(messageCode, new Object[] { param0 });
    	String sqlState = language.getSqlState(messageCode);
        return new SmallSQLException(message, sqlState);
    }

    static SQLException create( String messageCode, Object[] params ) {
    	String message = translateMsg(messageCode, params);
    	String sqlState = language.getSqlState(messageCode);
        return new SmallSQLException(message, sqlState);
    }
    
    static SQLException createFromException( Throwable e ){
        if(e instanceof SQLException) {
        	return (SQLException)e;
        }
        else {
        	String message = stripMsg(e);
        	String sqlState = language.getSqlState(Language.CUSTOM_MESSAGE);
        	return new SmallSQLException(e, message, sqlState);
        }
    }
    
    /**
	 * Create an exception with the specified message and appends the passed
	 * exception.<br>
	 * Makes use of localization. String type is used to avoid possible
	 * confusion with future implementations of Object[].
	 * 
	 * @param messageCode
	 *            localized message key. pass CUSTOM_MESSAGE and the plain
	 *            message as param0 to create an unlocalized message.
	 * @param param0
	 *            message parameter.
	 */
    static SQLException createFromException( String messageCode, Object param0, 
    		Throwable e )
    {
    	String message = translateMsg(messageCode, new Object[] { param0 });
    	String sqlState = language.getSqlState(messageCode);
        return new SmallSQLException(e, message, sqlState);
    }
    
	//////////////////////////////////////////////////////////////////////
	// MESSAGES ELABORATION METHODS
    // Every create* method uses one of these. 
	//////////////////////////////////////////////////////////////////////
    
	/**
	 * Get the localized message and format with the specified parameter,
	 * without creating an exception. Follows createMessage(String, Object[])
	 * convention.
	 * 
	 * @param messageCode
	 *            localized message key. pass CUSTOM_MESSAGE and the plain
	 *            message inside the parameters array to create an unlocalized
	 *            message.
	 * @param params
	 *            format parameters, nullable.
	 * @return translated message.
	 */
	static String translateMsg(String messageCode, Object[] params) {
		assert ( messageCode != null && params != null ): "Fill parameters. msgCode=" + messageCode + " params=" + params;
		
		String localized = language.getMessage(messageCode);		
		return MessageFormat.format(localized, params); 
	}

	/**
	 * Strips the message out of the passed exception, if possible.
	 */
	private static String stripMsg(Throwable throwable) {
		String msg = throwable.getMessage();
		if(msg == null || msg.length() < 30){
			String msg2 = throwable.getClass().getName();
			msg2 = msg2.substring(msg2.lastIndexOf('.')+1);
			if(msg != null)
				msg2 = msg2 + ':' + msg;
			return msg2;
		}
		
		return throwable.getMessage(); 
	}
}