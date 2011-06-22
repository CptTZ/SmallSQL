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
 * Language_it.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.database.language;

/**
 * Extended localization class for German language.
 */
public class Language_de extends Language {
	protected Language_de() {
		addMessages(ENTRIES);
	}
	
	public String[][] getEntries() {
		return ENTRIES;
	}
	
	//////////////////////////////////////////////////////////////////////
	// MESSAGES
	//////////////////////////////////////////////////////////////////////
	
    private final String[][] ENTRIES = {
            { UNSUPPORTED_OPERATION           , "Nicht unterstützte Funktion: {0}" },
            { CANT_LOCK_FILE                  , "Die Datei ''{0}'' kann nicht gelockt werden. Eine einzelne SmallSQL Datenbank kann nur für einen einzigen Prozess geöffnet werden." },

            { DB_EXISTENT                     , "Die Datenbank ''{0}'' existiert bereits." },
            { DB_NONEXISTENT                  , "Die Datenbank ''{0}'' existiert nicht." },
            { DB_NOT_DIRECTORY                , "Das Verzeichnis ''{0}'' ist keine SmallSQL Datenbank." },
            { DB_NOTCONNECTED                 , "Sie sind nicht mit einer Datenbank verbunden." },

            { CONNECTION_CLOSED               , "Die Verbindung ist bereits geschlossen." },

            { VIEW_INSERT                     , "INSERT wird nicht unterstützt für eine View." },
            { VIEWDROP_NOT_VIEW               , "DROP VIEW kann nicht mit ''{0}'' verwendet werden, weil es keine View ist." },
            { VIEW_CANTDROP                   , "View ''{0}'' kann nicht gelöscht werden." },

            { RSET_NOT_PRODUCED               , "Es wurde kein ResultSet erzeugt." },
            { RSET_READONLY                   , "Das ResultSet ist schreibgeschützt." },
            { RSET_FWDONLY                    , "Das ResultSet ist forward only." },
            { RSET_CLOSED                     , "Das ResultSet ist geschlossen." },
            { RSET_NOT_INSERT_ROW             , "Der Cursor zeigt aktuell nicht auf die Einfügeposition (insert row)." },
            { RSET_ON_INSERT_ROW              , "Der Cursor zeigt aktuell auf die Einfügeposition (insert row)." },
            { ROWSOURCE_READONLY              , "Die Rowsource ist schreibgeschützt." },
            { STMT_IS_CLOSED                  , "Das Statement ist bereits geschlossen." },

            { SUBQUERY_COL_COUNT              , "Die Anzahl der Spalten in der Subquery muss 1 sein und nicht {0}." },
            { JOIN_DELETE                     , "Die Methode deleteRow wird nicht unterstützt für Joins." },
            { JOIN_INSERT                     , "Die Methode insertRow wird nicht unterstützt für Joins." },
            { DELETE_WO_FROM                  , "Die Methode deleteRow benötigt einen FROM Ausdruck." },
            { INSERT_WO_FROM                  , "Die Methode insertRow benötigt einen FROM Ausdruck." },

            { TABLE_CANT_RENAME               , "Die Tabelle ''{0}'' kann nicht umbenannt werden." },
            { TABLE_CANT_DROP                 , "Die Tabelle ''{0}'' kann nicht gelöscht werden." },
            { TABLE_CANT_DROP_LOCKED          , "Die Tabelle ''{0}'' kann nicht gelöscht werden, weil sie gelockt ist." },
            { TABLE_CORRUPT_PAGE              , "Beschädigte Tabellenseite bei Position: {0}." },
            { TABLE_MODIFIED                  , "Die Tabelle ''{0}'' wurde modifiziert." },
            { TABLE_DEADLOCK                  , "Deadlock, es kann kein Lock erzeugt werden für Tabelle ''{0}''." },
            { TABLE_OR_VIEW_MISSING           , "Tabelle oder View ''{0}'' existiert nicht." },
            { TABLE_FILE_INVALID              , "Die Datei ''{0}'' enthält keine gültige SmallSQL Tabelle." },
            { TABLE_OR_VIEW_FILE_INVALID      , "Die Datei ''{0}'' ist keine gültiger Tabellen oder View Speicher." },
            { TABLE_EXISTENT                  , "Die Tabelle oder View ''{0}'' existiert bereits." },

            { FK_NOT_TABLE                    , "''{0}'' ist keine Tabelle." },
            { PK_ONLYONE                      , "Eine Tabelle kann nur einen Primärschlüssel haben." },
            { KEY_DUPLICATE                   , "Doppelter Schlüssel." },

            { MONTH_TOOLARGE                  , "Der Monat ist zu groß im DATE oder TIMESTAMP Wert ''{0}''." },
            { DAYS_TOOLARGE                   , "Die Tage sind zu groß im DATE oder TIMESTAMP Wert ''{0}''." },
            { HOURS_TOOLARGE                  , "Die Stunden sind zu groß im TIME oder TIMESTAMP Wert ''{0}''." },
            { MINUTES_TOOLARGE                , "Die Minuten sind zu groß im TIME oder TIMESTAMP Wert ''{0}''." },
            { SECS_TOOLARGE                   , "Die Sekunden sind zu groß im TIME oder TIMESTAMP Wert ''{0}''." },
            { MILLIS_TOOLARGE                 , "Die Millisekunden sind zu groß im TIMESTAMP Wert ''{0}''." },
            { DATETIME_INVALID                , "''{0}'' ist ein ungültiges DATE, TIME or TIMESTAMP." },

            { UNSUPPORTED_CONVERSION_OPER     , "Nicht unterstützte Konvertierung zu Datentyp ''{0}'' von Datentyp ''{1}'' für die Operation ''{2}''." },
            { UNSUPPORTED_DATATYPE_OPER       , "Nicht unterstützter Datentyp ''{0}'' für Operation ''{1}''." },
            { UNSUPPORTED_DATATYPE_FUNC       , "Nicht unterstützter Datentyp ''{0}'' für Funktion ''{1}''." },
            { UNSUPPORTED_CONVERSION_FUNC     , "Nicht unterstützte Konvertierung zu Datentyp ''{0}'' für Funktion ''{1}''." },
            { UNSUPPORTED_TYPE_CONV           , "Nicht unterstützter Typ für CONVERT Funktion: {0}." },
            { UNSUPPORTED_TYPE_SUM            , "Nicht unterstützter Datentyp ''{0}'' für SUM Funktion." },
            { UNSUPPORTED_TYPE_MAX            , "Nicht unterstützter Datentyp ''{0}'' für MAX Funktion." },
            { UNSUPPORTED_CONVERSION          , "Kann nicht konvertieren ''{0}'' [{1}] zu ''{2}''." },
            { INSERT_INVALID_LEN              , "Ungültige Länge ''{0}'' in Funktion INSERT." },
            { SUBSTR_INVALID_LEN              , "Ungültige Länge ''{0}'' in Funktion SUBSTRING." },

            { VALUE_STR_TOOLARGE              , "Der String Wert ist zu groß für die Spalte." },
            { VALUE_BIN_TOOLARGE              , "Ein Binäre Wert mit Länge {0} ist zu groß für eine Spalte mit der Größe {1}." },
            { VALUE_NULL_INVALID              , "Null Werte sind ungültig für die Spalte ''{0}''." },
            { VALUE_CANT_CONVERT              , "Kann nicht konvertieren ein {0} Wert zu einem {1} Wert." },

            { BYTEARR_INVALID_SIZE            , "Ungültige Bytearray Große {0} für UNIQUEIDENFIER." },
            { LOB_DELETED                     , "Lob Objekt wurde gelöscht." },

            { PARAM_CLASS_UNKNOWN             , "Unbekante Parameter Klasse: ''{0}''." },
            { PARAM_EMPTY                     , "Parameter {0} ist leer." },
            { PARAM_IDX_OUT_RANGE             , "Parameter Index {0} liegt außerhalb des Gültigkeitsbereiches. Der Wert muss zwischen 1 und {1} liegen." },

            { COL_DUPLICATE                   , "Es gibt einen doppelten Spaltennamen: ''{0}''." },
            { COL_MISSING                     , "Spalte ''{0}'' wurde nicht gefunden." },
            { COL_VAL_UNMATCH                 , "Die Spaltenanzahl und Werteanzahl ist nicht identisch." },
            { COL_INVALID_SIZE                , "Ungültige Spaltengröße {0} für Spalte ''{1}''." },
            { COL_WRONG_PREFIX                , "Der Spaltenprefix ''{0}'' passt zu keinem Tabellennamen oder Aliasnamen in dieser Abfrage." },
            { COL_READONLY                    , "Die Spalte {0} ist schreibgeschützt." },
            { COL_INVALID_NAME                , "Ungültiger Spaltenname ''{0}''." },
            { COL_IDX_OUT_RANGE               , "Spaltenindex außerhalb des Gültigkeitsbereiches: {0}." },
            { COL_AMBIGUOUS                   , "Die Spalte ''{0}'' ist mehrdeutig." },

            { GROUP_AGGR_INVALID              , "Aggregatfunktion sind nicht erlaubt im GROUP BY Klausel: ({0})." },
            { GROUP_AGGR_NOTPART              , "Der Ausdruck ''{0}'' ist nicht Teil einer Aggregatfunktion oder GROUP BY Klausel." },
            { ORDERBY_INTERNAL                , "Interner Error mit ORDER BY." },
            { UNION_DIFFERENT_COLS            , "Die SELECT Teile des UNION haben eine unterschiedliche Spaltenanzahl: {0} und {1}." },

            { INDEX_EXISTS                    , "Index ''{0}'' existiert bereits." },
            { INDEX_MISSING                   , "Index ''{0}'' existiert nicht." },
            { INDEX_FILE_INVALID              , "Die Datei ''{0}'' ist kein gültiger Indexspeicher." },
            { INDEX_CORRUPT                   , "Error beim Laden des Index. Die Index Datei ist beschädigt. ({0})." },
            { INDEX_TOOMANY_EQUALS            , "Zu viele identische Einträge im Index." },

            { FILE_TOONEW                     , "Dateiversion ({0}) der Datei ''{1}'' ist zu neu für diese Laufzeitbibliothek." },
            { FILE_TOOOLD                     , "Dateiversion ({0}) der Datei ''{1}'' ist zu alt für diese Laufzeitbibliothek." },
            { FILE_CANT_DELETE                , "Datei ''{0}'' kann nicht gelöscht werden." },

            { ROW_0_ABSOLUTE                  , "Datensatz 0 ist ungültig für die Methode absolute()." },
            { ROW_NOCURRENT                   , "Kein aktueller Datensatz." },
            { ROWS_WRONG_MAX                  , "Fehlerhafter Wert für Maximale Datensatzanzahl: {0}." },
            { ROW_LOCKED                      , "Der Datensatz ist gelocked von einer anderen Verbindung." },
            { ROW_DELETED                     , "Der Datensatz ist bereits gelöscht." },

            { SAVEPT_INVALID_TRANS            , "Der Savepoint ist nicht gültig für die aktuelle Transaction." },
            { SAVEPT_INVALID_DRIVER           , "Der Savepoint ist nicht gültig für diesen Treiber {0}." },

            { ALIAS_UNSUPPORTED               , "Ein Alias ist nicht erlaubt für diesen Typ von Rowsource." },
            { ISOLATION_UNKNOWN               , "Unbekantes Transaktion Isolation Level: {0}." },
            { FLAGVALUE_INVALID               , "Ungültiger Wert des Flags in Methode getMoreResults: {0}." },
            { ARGUMENT_INVALID                , "Ungültiges Argument in Methode setNeedGenratedKeys: {0}." },
            { GENER_KEYS_UNREQUIRED           , "GeneratedKeys wurden nicht angefordert." },
            { SEQUENCE_HEX_INVALID            , "Ungültige Hexadecimal Sequenze bei Position {0}." },
            { SEQUENCE_HEX_INVALID_STR        , "Ungültige Hexadecimal Sequenze bei Position {0} in ''{1}''." },

            { SYNTAX_BASE_OFS                 , "Syntax Error bei Position {0} in ''{1}''. " },
            { SYNTAX_BASE_END                 , "Syntax Error, unerwartetes Ende des SQL Strings. " },
            { STXADD_ADDITIONAL_TOK           , "Zusätzliche Zeichen nach dem Ende des SQL statement." },
            { STXADD_IDENT_EXPECT             , "Bezeichner erwartet." },
            { STXADD_IDENT_EMPTY              , "Leerer Bezeichner." },
            { STXADD_IDENT_WRONG              , "Ungültiger Bezeichner ''{0}''." },
            { STXADD_OPER_MINUS               , "Ungültiger Operator Minus für Datentyp VARBINARY." },
            { STXADD_FUNC_UNKNOWN             , "Unbekannte Funktion." },
            { STXADD_PARAM_INVALID_COUNT      , "Ungültige Paramter Anzahl." },
            { STXADD_JOIN_INVALID             , "Ungültige Join Syntax." },
            { STXADD_FROM_PAR_CLOSE           , "Unerwartet schließende Klammer in FROM Klausel." },
            { STXADD_KEYS_REQUIRED            , "Benötige Schlüsselwörter sind: " },
            { STXADD_NOT_NUMBER               , "Eine Zahl ist erforderlich: ''{0}''." },
            { STXADD_COMMENT_OPEN             , "Fehlendes Kommentarende ''*/''." },
    };
}