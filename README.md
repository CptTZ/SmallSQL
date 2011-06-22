SmallSQL Database 
Version 0.21

Copyright 2004-2011, by Volker Berlin
Support: http://www.smallsql.de/support.html

Introduction
=============
SmallSQL Database is a free DBMS library for the Java(tm) platform.  It runs 
on the Java 2 Platform (JDK 1.4 or later) and implements the JDBC 3.0 API.

SmallSQL Database is licensed under the terms of the GNU Lesser General
Public License (LGPL).  A copy of the license is included in the
distribution.

Please note that SmallSQL Database is distributed WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.  Please refer to the license for details.


Installation
=============
See online at http://www.smallsql.de/doc/install.htm
or in documentation download at doc\install.htm


Changes in Version 0.21 (2011-06-22)
====================================
- ResultSet.getBinaryStream() implemented.
- Added function year().
- Fix conversion of binary data to String (bug 3001088).
- Add JDBC URL property readonly. With readonly it is possible to share 
  one database between multiple Java processes.
- Fix a thread problem with RandomAccessFile.   



Changes in Version 0.20 (2008-12-14)
====================================
- Add AUTO_INCREMENT as alias for IDENTITY.
- Add the keyword LIMIT as alias for the TOP syntax of a SELECT
- Improve the handling from IDENTITY values (bug 1954682).
- Fix a bug with different writing of the same database.
- Fix a thread bug with concurrent reading of the same table.
- Fix an ArrayIndexOutOfBoundsException with large values >32K (bug 2264600).
- CREATE TABLE and CREATE VIEW are now in a transaction. 
  This make a rollback possible. (bug 2256579).



Changes in Version 0.19 (2007-07-31)
====================================
- Add support for multi language error messages. Support for English, 
  Italian and German was added.
- Improve the performance of a simple JOIN.
- Double columns in an INSERT throw an SQLException now (bug 1735908).
- Date range check improved (bug 1738435).
- Check for ambiguous columns added.
- SmallSQL verify now that a database is only open once.
- Added SQL comments symbols, both single (" -- comment ") and multi-line
  (" /* comment */ ").
- A NullPointerException with a comma at end of the SQL (bug 1745881).
- Check the close state of a Statement in many methods (bug 1753529).
- The keyword INNER in the JOIN syntax is optional now (feature 1753519).
- Added SQL functions: BIT_LENGTH(string), CHAR_LENGTH(string), 
  CHARACTER_LENGTH(string), OCTET_LENGTH(string) and CURRENT_DATE(). 



Changes in Version 0.18 (2007-06-10)
====================================
- Fix bugs with the methods isBeforeFirst(), isFirst(), isLast() and isAfterLast().
  The problems occur mostly with empty ResultSets and together with ORDER BY or GROUP BY.
- Fix the scrolling of scrollable, updatable ResultSets with inserted rows.
- Signal inserted and rollbacked rows as deleted (method rowDeleted()).
- A NullPointerException with flag create=true was fixed.
- Check the range of the parsed date time parts now (bug 1731080).
- Double columns in a table are now prevented. It was possible 
  with CREATE TABLE and ALTER TABLE to create doubled columns (bug 1731088).
- Unicode characters are possible in JDBC URL parameters now (bug 1732416).
- The method getPropertyInfo is implemented now.
  


Changes in Version 0.17 (2007-02-15)
====================================
- STUFF as alias for the INSERT SQL function added.
- ALTER TABLE was added.
- A reading bug of DateTime values with daylight saving was fixed.
- A SQL parser bug with cross joins and aliases was fixed. 
  Now also the second table can have an alias (bug 1624376).
- Support for the method relative, absolute, isBeforeFirst and isFirst
  for Scrollable ResultSets with ORDER BY was added (bug 1625080).
- Now a "SELECT TOP 0 ..." returns 0 rows instead of all rows (bug 1629244).
- A bug with LIKE and the wildcard % at end of the pattern was fixed (bug 1647564).
- The driver ignores now right blanks if a CHAR value is compared with a VARCHAR value (bug 1654121).



Changes in Version 0.16 (2006-10-21)
====================================
- A ConcurrentModificationException was fixed on Connection.close().
- Command line tool was added.
- A call of getMoreResults() has not change the results from getUpdateCount() and getResultSet().



Changes in Version 0.15 (2006-06-28)
====================================
- The HAVING clause has not work.
- A bug with date time functions in GROUP BY clause was fixed.
- Wrong ResultSetMetaData with GROUP BY was fixed.
- The follow SQL functions was added:
	* CHAR
	* DIFFERENCE
	* INSERT
	* LCASE
	* LEFT
	* LTRIM
	* REPEAT
	* REPLACE
	* RTRIM
	* SOUNDEX
	* SPACE
	* UCASE
- The number of JUnit tests was increased greatly. 
  There is now a block code coverage of 80% with the EMMA tool.


Changes in Version 0.14 (2006-05-20)
====================================
- Some bugs for multiple execution of a PreparedStatment was fixed.
- The method getGeneratedKeys() and related method was implemented.
- The batch processing to the Statement class was implemented. 
  Before is was only implements for PreparedStatement.
- Some methods of the interface Statement was implemented.
- Many methods of the ResultSet interface was implemented or corrected. 
  For example the methods for the scroll status.
- The wrong ResultSetMetaData of the ResultSets from
  the DatabaseMetaData methods was fixed.
- There is now a block code coverage of 75% with the EMMA tool.


Changes in Version 0.13 (2006-04-09)
====================================
- The precision for varchar and varbinary was saved wrong with 38 before.
- Negativ numeric and decimal values was rounded wrong with the getInt() method.
- The prefix "file:" on the database name will be ignored now. This is valid 
  for the methods getConnection() and setCatalog() and for the SQL commands
  CREATE DATABASE, DROP DATABASE and USE DATABASE.
- The URL property "dbpath" and "create" was added.


Changes in Version 0.12 (2006-03-05)
=====================================
- Fix for CONVERT function with timestamp values.
- Fix for Date values toString() with values before 1970.
- Follow SQL function was added:
	* DAYOFWEEK
	* DAYOFYEAR
	* DAYOFMONTH
	* CURDATE
	* CURTIME
	* MONTH
	* HOUR
	* MINUTE
- Fix for ConcurrentModificationException on Connection.close().
- SQL Syntax enhancement for CREATE TABLE. A index can be add together with the column description.
- Fix for a NPE on INSERT TABLE with tables that have a index.
- Now delete also the lob data file if the table is drop.


Changes in Version 0.11 (2006-02-11)
=====================================
- Fix for NPE with DatabaseMetaData.getTables() in not connect mode for JDBC Navigator.
- Free the cached resources of a database if all connection to this database are closed.
- A more helpful exception if a connection is closed instead of a NPE.
- Fix for the error "Invalide column size:-1" for the data types 
  LONGVARCHAR, LONGVARBINARY, VARCHAR  larger 127 characters and VARBINARY larger 127 bytes.


Chnages in Version 0.10 (2006-02-05)
=====================================
- Driver classname was changed vom smallsql.server.SSDriver to smallsql.database.SSDriver
- Fileformat was changed.
- Follow SQL functions was added: 
	* ACOS
	* ASIN
	* ATAN
	* ATAN2
	* CEILING
	* COS
	* COT
	* DEGREES
	* EXP
	* FLOOR
	* LOG
	* LOG10
	* MOD
	* PI
	* POWER
	* RADIANS
	* RAND
	* ROUND
	* SIGN
	* SQRT
	* SIN
	* TAN
	* TRUNCATE


Changes in Version 0.02 (2005-05-22)
=====================================
- The most DatabaseMetaData are implement now.
- Bug fixes for the JDBC Navigator (http://home.planet.nl/~demun000/thomas_projects/jdbcnav).
- Bug fixes for the DbVisualizer 4.2.


Changes in Version 0.01 (2005-04-16)
=====================================
- First public version