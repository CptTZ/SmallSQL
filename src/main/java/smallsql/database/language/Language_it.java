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
 * Author: Saverio Miroddi
 * 
 */
package smallsql.database.language;

/**
 * Extended localization class for Italian language.
 */
public class Language_it extends Language {
	protected Language_it() {
		addMessages(ENTRIES);
	}
	
	public String[][] getEntries() {
		return ENTRIES;
	}
	
	//////////////////////////////////////////////////////////////////////
	// MESSAGES
	//////////////////////////////////////////////////////////////////////
	
	private final String[][] ENTRIES = {
{ UNSUPPORTED_OPERATION           , "Operazione non supportata: {0}." },
{ CANT_LOCK_FILE                  , "Impossibile bloccare il file ''{0}''. Un database SmallSQL Database può essere aperto da un unico processo." },

{ DB_EXISTENT                     , "Il database ''{0}'' è già esistente." },
{ DB_NONEXISTENT                  , "Il database ''{0}'' Non esiste." },
{ DB_NOT_DIRECTORY                , "La directory ''{0}'' non è un database SmallSQL." },
{ DB_NOTCONNECTED                 , "L''utente non è connesso a un database." },

{ CONNECTION_CLOSED               , "La connessione è già chiusa." },

{ VIEW_INSERT                     , "INSERT non è supportato per una view." },
{ VIEWDROP_NOT_VIEW               , "Non è possibile effettuare DROP VIEW con ''{0}'' perché non è una view." },
{ VIEW_CANTDROP                   , "Non si può effettuare drop sulla view ''{0}''." },

{ RSET_NOT_PRODUCED               , "Nessun ResultSet è stato prodotto." },
{ RSET_READONLY                   , "Il ResultSet è di sola lettura." },
{ RSET_FWDONLY                    , "Il ResultSet è forward only." }, // no real translation
{ RSET_CLOSED                     , "Il ResultSet è chiuso." },
{ RSET_NOT_INSERT_ROW             , "Il cursore non è attualmente nella riga ''InsertRow''." },
{ RSET_ON_INSERT_ROW              , "Il cursore è attualmente nella riga ''InsertRow''." },
{ ROWSOURCE_READONLY              , "Il Rowsource è di sola lettura." },

{ STMT_IS_CLOSED                  , "Lo Statement è in stato chiuso." },

{ SUBQUERY_COL_COUNT              , "Il conteggio delle colonne nella subquery deve essere 1 e non {0}." },
{ JOIN_DELETE                     , "DeleteRow non supportato nelle join." },
{ JOIN_INSERT                     , "InsertRow non supportato nelle join." },
{ DELETE_WO_FROM                  , "DeleteRow necessita un''espressione FROM." },
{ INSERT_WO_FROM                  , "InsertRow necessita un''espressione FROM." },

{ TABLE_CANT_RENAME               , "La tabella ''{0}'' non può essere rinominata." },
{ TABLE_CANT_DROP                 , "Non si può effettuare DROP della tabella ''{0}''." },
{ TABLE_CANT_DROP_LOCKED          , "Non si può effettuare DROP della tabella ''{0}'' perché è in LOCK." },
{ TABLE_CORRUPT_PAGE              , "Pagina della tabella corrotta alla posizione: {0}." },
{ TABLE_MODIFIED                  , "La tabella ''{0}'' è stata modificata." },
{ TABLE_DEADLOCK                  , "Deadlock: non si può mettere un lock sulla tabella ''{0}''." },
{ TABLE_OR_VIEW_MISSING           , "La tabella/view ''{0}'' non esiste." },
{ TABLE_FILE_INVALID              , "Il file ''{0}'' non include una tabella SmallSQL valida." },
{ TABLE_OR_VIEW_FILE_INVALID      , "Il file ''{0}'' non è un contenitore valido di tabella/view." },
{ TABLE_EXISTENT                  , "La tabella/vista ''{0}'' è già esistente." },

{ FK_NOT_TABLE                    , "''{0}'' non è una tabella." },
{ PK_ONLYONE                      , "Una tabella può avere solo una primary key." },
{ KEY_DUPLICATE                   , "Chiave duplicata." },

{ MONTH_TOOLARGE                  , "Valore del mese troppo alto del in DATE o TIMESTAMP ''{0}''." },
{ DAYS_TOOLARGE                   , "Valore del giorno troppo altro in DATE o TIMESTAMP ''{0}''." },
{ HOURS_TOOLARGE                  , "Valore delle ore troppo alto in in TIME o TIMESTAMP ''{0}''." },
{ MINUTES_TOOLARGE                , "Valore dei minuti troppo alto in TIME o TIMESTAMP ''{0}''." },
{ SECS_TOOLARGE                   , "Valore dei secondi troppo alto in TIME o TIMESTAMP ''{0}''." },
{ MILLIS_TOOLARGE                 , "VAlore dei millisecondi troppo alto in TIMESTAMP ''{0}''." },
{ DATETIME_INVALID                , "''{0}'' è un DATE, TIME or TIMESTAMP non valido." },

{ UNSUPPORTED_CONVERSION_OPER     , "Conversione non supportata verso il tipo di dato ''{0}'' dal tipo ''{1}'' per l''operazione ''{2}''." },
{ UNSUPPORTED_DATATYPE_OPER       , "Tipo di dato ''{0}'' non supportato per l''operazione ''{1}''." },
{ UNSUPPORTED_DATATYPE_FUNC       , "Tipo di dato ''{0}'' non supportato per la funzione ''{1}''." },
{ UNSUPPORTED_CONVERSION_FUNC     , "Conversione verso il tipo di dato ''{0}'' non supportato per la funzione ''{1}''." },
{ UNSUPPORTED_TYPE_CONV           , "Tipo non supportato per la funzione CONVERT: {0}." },
{ UNSUPPORTED_TYPE_SUM            , "Tipo non supportato per la funzione SUM: ''{0}''." },
{ UNSUPPORTED_TYPE_MAX            , "Tipo non supportato per la funzione MAX: ''{0}''." },
{ UNSUPPORTED_CONVERSION          , "Non è possible convertire ''{0}'' [{1}] in ''{2}''." },
{ INSERT_INVALID_LEN              , "Lunghezza non valida ''{0}'' per la funzione INSERT." },
{ SUBSTR_INVALID_LEN              , "Lunghezza non valida ''{0}'' per la funzione SUBSTRING." },

{ VALUE_STR_TOOLARGE              , "Stringa troppo lunga per la colonna." },
{ VALUE_BIN_TOOLARGE              , "Valore binario di lunghezza {0} eccessiva per la colonna di lunghezza {1}." },
{ VALUE_NULL_INVALID              , "Valori nulli non validi per la colonna ''{0}''." },
{ VALUE_CANT_CONVERT              , "Impossible convertire un valore {0} in un valore {1}." },

{ BYTEARR_INVALID_SIZE            , "Lunghezza non valida per un array di bytes: {0}." },
{ LOB_DELETED                     , "L''oggetto LOB è stato cancellato." },

{ PARAM_CLASS_UNKNOWN             , "Classe sconosciuta (''{0}'') per il parametro." },
{ PARAM_EMPTY                     , "Il parametro {0} è vuoto." },
{ PARAM_IDX_OUT_RANGE             , "L''indice {0} per il parametro è fuori dall''intervallo consentito ( 1 <= n <= {1} )." },

{ COL_DUPLICATE                	  , "Nome di colonna duplicato: ''{0}''." },
{ COL_MISSING                     , "Colonna ''{0}'' non trovata." },
{ COL_VAL_UNMATCH                 , "Il conteggio di colonne e valori non è identico." },
{ COL_INVALID_SIZE                , "Lunghezza non valida ({0}) per la colonna ''{1}''." },
{ COL_WRONG_PREFIX                , "Il prefisso di colonna ''{0}'' non coincide con un alias o nome di tabella usato nella query." },
{ COL_READONLY                    , "La colonna ''{0}'' è di sola lettura." },
{ COL_INVALID_NAME                , "Nome di colonna non valido ''{0}''." },
{ COL_IDX_OUT_RANGE               , "Indice di colonna fuori dall''intervallo valido: {0}." },
{ COL_AMBIGUOUS                   , "Il nome di colonna ''{0}'' è ambiguo." },

{ GROUP_AGGR_INVALID              , "Funzione di aggregrazione non valida per la clausola GROUP BY: ({0})." },
{ GROUP_AGGR_NOTPART              , "L''espressione ''{0}'' non è parte di una funzione di aggregazione o della clausola GROUP BY." },
{ ORDERBY_INTERNAL                , "Errore interno per ORDER BY." },
{ UNION_DIFFERENT_COLS            , "SELECT appartenenti ad una UNION con numero di colonne differenti: {0} e {1}." },

{ INDEX_EXISTS                    , "L''indice ''{0}'' è già esistente." },
{ INDEX_MISSING                   , "L''indice ''{0}'' non esiste." },
{ INDEX_FILE_INVALID              , "Il file ''{0}'' non è un contenitore valido per un indice." },
{ INDEX_CORRUPT                   , "Errore durante il caricamento dell''indice. File dell''indice corrotto: ''{0}''." },
{ INDEX_TOOMANY_EQUALS            , "Troppe voci uguali nell''indice." },

{ FILE_TOONEW                     , "La versione ({0}) del file ''{1}'' è troppo recente per questo runtime." },
{ FILE_TOOOLD                     , "La versione ({0}) del file ''{1}'' è troppo vecchia per questo runtime." },
{ FILE_CANT_DELETE                , "File ''(0)'' non possono essere eliminati." },

{ ROW_0_ABSOLUTE                  , "Il numero di riga 0 non è valido per il metodo ''absolute()''." },
{ ROW_NOCURRENT                   , "Nessuna riga corrente." },
{ ROWS_WRONG_MAX                  , "Numero massimo di righe non valido ({0})." },
{ ROW_LOCKED                      , "La riga è bloccata da un''altra connessione." },
{ ROW_DELETED                     , "Riga già cancellata." },

{ SAVEPT_INVALID_TRANS            , "SAVEPOINT non valido per questa transazione." },
{ SAVEPT_INVALID_DRIVER           , "SAVEPOINT non valido per questo driver {0}." },

{ ALIAS_UNSUPPORTED               , "Alias non supportato per questo tipo di sorgente righe." },
{ ISOLATION_UNKNOWN               , "Livello di Isolamento transazione sconosciuto: {0}." },
{ FLAGVALUE_INVALID               , "Flag non valida nel metodo ''getMoreResults'': {0}." },
{ ARGUMENT_INVALID                , "Argomento non valido nel metodo ''setNeedGenratedKeys'': {0}." },
{ GENER_KEYS_UNREQUIRED           , "GeneratedKeys non richieste." },
{ SEQUENCE_HEX_INVALID            , "Sequenza esadecimale non valido alla posizione {0}." },
{ SEQUENCE_HEX_INVALID_STR        , "Sequence esadecimale non valida alla positione {0} in ''{1}''." },

{ SYNTAX_BASE_OFS            	  , "Errore di sintassi alla posizione {0} in ''{1}''. " },
{ SYNTAX_BASE_END        		  , "Errore di sintassi, fine inattesa della stringa SQL. " },
{ STXADD_ADDITIONAL_TOK			  , "Token aggiuntivo dopo la fine dell''istruzione SQL." },
{ STXADD_IDENT_EXPECT			  , "Identificatore atteso." },
{ STXADD_IDENT_EMPTY 			  , "Identificatore vuoto." },
{ STXADD_IDENT_WRONG 			  , "Identificatore errato ''{0}''." },
{ STXADD_OPER_MINUS 			  , "Operatore ''meno'' non valido per il tipo di dato varbinary." },
{ STXADD_FUNC_UNKNOWN 			  , "Funzione sconosciuta." },
{ STXADD_PARAM_INVALID_COUNT	  , "Totale parametri non valido." },
{ STXADD_JOIN_INVALID	  		  , "Sintassi della join non valida." },
{ STXADD_FROM_PAR_CLOSE	  		  , "Parentesi chiusa non attesa nella clausola from." },
{ STXADD_KEYS_REQUIRED	  		  , "Le parole chiave richieste sono: " },
{ STXADD_NOT_NUMBER	  		      , "Richiesto valore numerico (passato = ''{0}'')." },
{ STXADD_COMMENT_OPEN	  		  , "Chiusura del commento mancante (''*/'')." },
	};
}