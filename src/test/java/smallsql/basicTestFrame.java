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
 * AllTests.java
 * ---------------
 * Author: Volker Berlin
 *
 */
package smallsql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Basic test frame that contains creating new connection
 */
public class basicTestFrame {

    public final static String CATALOG = "db1";
    public final static String JDBC_URL = "jdbc:smallsql:" + CATALOG;
    private static Connection con;

    public static Connection getConnection() throws SQLException {
        if (con == null || con.isClosed()) {
            con = createConnection();
        }
        return con;
    }

    /**
     * Creates a connection in the English locale.<br>
     */
    public static Connection createConnection() throws SQLException {
        //DriverManager.setLogStream( System.out );
        new smallsql.database.SSDriver();
        return DriverManager.getConnection(JDBC_URL + "?create=true;locale=en");
        //return DriverManager.getConnection("jdbc:odbc:mssql","sa","");
    }

    /**
     * Creates a connection, with the possibility of appending an additional
     * string to the url and/or passing a Properties object.<br>
     * Locale is not specified.
     *
     * @param urlAddition String to append to url; nullable.
     * @param info        object Properties; nullable.
     * @return connection created.
     */
    public static Connection createConnection(String urlAddition, Properties info) throws SQLException {
        new smallsql.database.SSDriver();

        if (urlAddition == null) urlAddition = "";
        if (info == null) info = new Properties();

        String urlComplete = JDBC_URL + urlAddition;

        return DriverManager.getConnection(urlComplete, info);
    }

    public static void printRS(ResultSet rs) throws SQLException {
        while (rs.next()) {
            for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                System.out.print(rs.getObject(i) + "\t");
            }
            System.out.println();
        }
    }

}
