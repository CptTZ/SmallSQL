/* =============================================================
 * SmallSQL : a free Java DBMS library for the Java(tm) platform
 * =============================================================
 *
 * (C) Copyright 2004-2011, by Volker Berlin.
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
 * StoreImpl.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.database;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.SQLException;
import smallsql.database.language.Language;

public class StoreImpl extends Store {

    private static final int DEFAULT_PAGE_SIZE = 8192; // 8 Kb
	private static final int PAGE_MAGIC = 0x12DD13DE; // are used for repairing a table
    /**
     * The structure of the Page Control Block is:
     * 4 byte - page magic
     * 4 Byte - Status 0:normal; 1:deleted; 2:Pointer to an update; 3: updated page
     * 4 Byte - used size of the page
     * 4 byte - physical size of the page
     * 4 byte - offset to the next page
     * 8 byte - position of an updated page
     */
	private static final int PAGE_CONTROL_SIZE = 28;
	private static final byte[] page_control = new byte[PAGE_CONTROL_SIZE]; 
	private static final ByteBuffer pageControlBuffer = ByteBuffer.wrap(page_control); 
	private int status; // valid value are follow:
	private static final int NORMAL = 0;
    private static final int DELETED = 1;
    /**
     * Using of UPDATE_POINTER and UPDATED_PAGE
     * 	If a page are updated and the new data are larger as the old data then
     *  the old page are changed to a UPDATE_POINTER. The new page is 
     *  a UPDATED_PAGE. On reading the pages only the UPDATE_POINTER is read.
     *  The UPDATED_PAGE are skipped. Thats a row change not it position.
     */
	private static final int UPDATE_POINTER = 2;
	private static final int UPDATED_PAGE = 3;
    
    final private Table table;
    /** Data of one page */
    private byte[] page; 
    /** if the current array "page" is shared. This make sence for read only access but not if it will be write. */
    private boolean sharedPageData;
    private StorePage storePage;
    private long filePos; // Position in the file
    private int sizeUsed;
    private int sizePhysical;
    private int nextPageOffset;
    private long filePosUpdated;
    private int type;
    
    private StoreImpl updatePointer;

    private StoreImpl( Table table, StorePage storePage, int type, long filePos ){
		this.table     = table;
		this.storePage    = storePage;
		this.filePos   = filePos;
		this.type      = type;
    }
    
    
    /**
     * Follow types of StoreImpl are possible:
     * INSERT: A page that will be include new data. filePos is not define yet.
     * CREATE: A special type of INSERT
     * SELECT: Only read operations are possible.
     * UPDATE: Has a filePos, if the new size is to small then the old page must be deleted and a new added.
     * DELETE: Has no cache else only the filePos to write the flag.
    */
    static StoreImpl createStore( Table table, StorePage storePage, int type, long filePos ) throws SQLException{
        try {
            StoreImpl store = new StoreImpl(table, storePage, type, filePos);
            switch(type){
                case SQLTokenizer.LONGVARBINARY:
                    // is used for storing of LONGVARBINARY and LONGVARCHAR
                    store.page = new byte[(int)filePos + PAGE_CONTROL_SIZE];
                    store.filePos = -1;
                    break;
                case SQLTokenizer.INSERT:
                case SQLTokenizer.CREATE:
                    store.page = new byte[DEFAULT_PAGE_SIZE];
                    break;
                case SQLTokenizer.SELECT:
                case SQLTokenizer.UPDATE:
            	case SQLTokenizer.DELETE:
                    if(storePage.page == null){
                        FileChannel raFile = storePage.raFile;
                        synchronized(raFile){
                            if(filePos >= raFile.size() - PAGE_CONTROL_SIZE){
                                return null;
                            }
                            raFile.position(filePos);
                            synchronized(page_control){
                                pageControlBuffer.position(0);
                                raFile.read(pageControlBuffer);
                                store.page = page_control;
                                store.readPageHeader();
                            }
                            store.page = new byte[store.sizeUsed];
                            raFile.position(filePos);
                            ByteBuffer buffer = ByteBuffer.wrap(store.page);
                            raFile.read(buffer);
                        }
                    }else{
                        store.page = storePage.page;
                        store.sharedPageData = true;
                        store.readPageHeader();
                    }
                    store = store.loadUpdatedStore();
                    break;
                default: throw new Error();
            }
            store.offset = PAGE_CONTROL_SIZE;
            return store;
        } catch (Throwable th) {
            throw SmallSQLException.createFromException(th);
        }
    }
    
    
    /**
     * Recreate a StoreImpl from an uncommitted StorePage.
     */
	static StoreImpl recreateStore( Table table, StorePage storePage, int type) throws Exception{
		StoreImpl store = new StoreImpl(table, storePage, type, -1);
		store.page = storePage.page;
		store.sharedPageData = true;
		store.readPageHeader();
		store = store.loadUpdatedStore();
		store.offset = PAGE_CONTROL_SIZE;
		return store;
	}
	
    
    private final void readPageHeader() throws SQLException{
		if(readInt() != PAGE_MAGIC){
			throw SmallSQLException.create(Language.TABLE_CORRUPT_PAGE, new Object[] { new Long(filePos) });
		}
		status = readInt();
		sizeUsed  = readInt();
		sizePhysical = readInt();
		nextPageOffset = readInt();
		filePosUpdated = readLong();
    }
    
    
    /**
     * If the current page is only an pointer to a larger page then the larger page is loaded else it return this.
     * @return this or the page with the valid data.
     * @throws Exception
     */
	final private StoreImpl loadUpdatedStore() throws Exception{
		if(status != UPDATE_POINTER) return this;
		StoreImpl storeTemp = table.getStore( ((TableStorePage)storePage).con, filePosUpdated, type);
		storeTemp.updatePointer = this;
		return storeTemp;
    }
    

    private void resizePage(int minNewSize){
    	int newSize = Math.max(minNewSize, page.length*2);
    	byte[] newPage = new byte[newSize];
    	System.arraycopy( page, 0, newPage, 0, page.length);
    	page = newPage;
    }
    

	@Override
    boolean isValidPage(){
		return status == NORMAL || (status == UPDATED_PAGE && updatePointer != null); 
	}
	
    @Override
    int getUsedSize(){
        return sizeUsed;
    }
    
    @Override
    long getNextPagePos(){
    	if(updatePointer != null) return updatePointer.getNextPagePos();
    	if(nextPageOffset <= 0){
			nextPageOffset = sizePhysical; 
    	}
		return filePos + nextPageOffset;
    }

    
    /**
     * 
     * @param con Is needed to add this page to the commitPages. If it null then it save directly without rollback option.
     * @return The file position if con == null.
     * @throws SQLException
     */
    long writeFinsh(SSConnection con) throws SQLException{
        switch(type){
            case SQLTokenizer.LONGVARBINARY:
            case SQLTokenizer.INSERT:
            case SQLTokenizer.CREATE:
                sizeUsed = sizePhysical = offset;
                break;
			case SQLTokenizer.UPDATE:
				if(status != UPDATE_POINTER) {
					sizeUsed = offset;
					break;
				}
            case SQLTokenizer.DELETE:
				sizeUsed = PAGE_CONTROL_SIZE;
                break;
            //SQLTokenizer.SELECT should not occur here
            default: throw new Error(""+type);
        }
		offset = 0;
		writeInt( PAGE_MAGIC ); // for repair 
		writeInt( status);
		writeInt( sizeUsed );
		writeInt( sizePhysical );
		writeInt( 0 ); //nextPageOffset
		writeLong( filePosUpdated ); // Pointer of an updated page
		storePage.setPageData( page, sizeUsed ); //TODO page sollte eigentlich beim einlesen gesetzt sein
        if(con == null){
        	// the pointer is needed to safe in another page
        	// this produce not referenced pages on rollback
			return storePage.commit();
        }else{
            return 0;
        }
    }
    
    
    /**
     * Request a write lock for the current read page. It add the resulting StorePage to the list of commits.
     * 
     * @throws SQLException
     *             if for the current row (page) cannot create a write lock or th connection was closed.
     */
    final void createWriteLock() throws SQLException{
		TableStorePage storePageWrite = table.requestWriteLock( ((TableStorePage)storePage).con, (TableStorePage)storePage );
		if(storePageWrite == null)
			throw SmallSQLException.create(Language.ROW_LOCKED);
		storePage = storePageWrite;
    }
    
    /**
     * Is call from updateRow().
     * The offset of newData must be at the end of the data. It used as new page size. 
     */
	void updateFinsh(SSConnection con, StoreImpl newData) throws SQLException{
		type = SQLTokenizer.UPDATE;
		//createWriteLock();
		if(newData.offset <= sizePhysical || filePos == -1){
			// the old page can be overwrite because it it large enough
			page = newData.page; //newData is only a temp StoreImpl
			offset = newData.offset;
			if(sizePhysical < offset) sizePhysical = offset; // occur only on updates of not committed inserts (filePos == -1
			writeFinsh(con);
		}else{
			// we need to create a new page because the old page is to small
			newData.status = UPDATED_PAGE;
			if(updatePointer == null){
				// we need to create a new page and change the old page to a UPDATE_POINTER
				((TableStorePage)newData.storePage).lockType = TableView.LOCK_INSERT;
				filePosUpdated = newData.writeFinsh(null);
				status = UPDATE_POINTER;
			}else{
				// we need to create a new page and delete the old page
				((TableStorePage)newData.storePage).lockType = TableView.LOCK_INSERT;
				updatePointer.filePosUpdated = newData.writeFinsh(null);
				updatePointer.status = UPDATE_POINTER;
				updatePointer.type = SQLTokenizer.UPDATE;
				updatePointer.createWriteLock();
				if(updatePointer.sharedPageData){
				    updatePointer.page = new byte[PAGE_CONTROL_SIZE];
				}
				updatePointer.writeFinsh(con);
				status = DELETED;
                if(sharedPageData){
                    page = new byte[PAGE_CONTROL_SIZE];
                }
			}
			writeFinsh(con);
		}
	}
    
/*==============================================================================

Write und Read Methoden

==============================================================================*/
    private int offset; // aktuelle read/write Position in der Page
	
	
	int getCurrentOffsetInPage(){
		return offset;
	}
	
	
	void setCurrentOffsetInPage(int newOffset){
		this.offset = newOffset;
	}
	

    void writeByte( int value ){
    	int newSize = offset + 1;
        if(newSize > page.length) resizePage(newSize);

        page[ offset++ ] = (byte)(value);
    }

    int readByte(){
        return page[ offset++ ];
    }

    int readUnsignedByte(){
        return page[ offset++ ] & 0xFF;
    }

    void writeBoolean( boolean value ){
    	int newSize = offset + 1;
        if(newSize > page.length) resizePage(newSize);

        page[ offset++ ] = (byte)(value ? 1 : 0);
    }

    boolean readBoolean(){
        return page[ offset++ ] != 0;
    }

    void writeShort( int value ){
    	int newSize = offset + 2;
        if(newSize > page.length) resizePage(newSize);

        page[ offset++ ] = (byte)(value >> 8);
        page[ offset++ ] = (byte)(value);
    }

    int readShort(){
        return (page[ offset++ ] << 8) | (page[ offset++ ] & 0xFF);
    }

    void writeInt( int value ){
    	int newSize = offset + 4;
        if(newSize > page.length) resizePage(newSize);

        page[ offset++ ] = (byte)(value >> 24);
        page[ offset++ ] = (byte)(value >> 16);
        page[ offset++ ] = (byte)(value >> 8);
        page[ offset++ ] = (byte)(value);
    }

    int readInt(){
        return  ((page[ offset++ ]) << 24) |
                ((page[ offset++ ] & 0xFF) << 16) |
                ((page[ offset++ ] & 0xFF) << 8) |
                ((page[ offset++ ] & 0xFF));
    }

    void writeLong( long value ){
    	int newSize = offset + 8;
        if(newSize > page.length) resizePage(newSize);

        page[ offset++ ] = (byte)(value >> 56);
        page[ offset++ ] = (byte)(value >> 48);
        page[ offset++ ] = (byte)(value >> 40);
        page[ offset++ ] = (byte)(value >> 32);
        page[ offset++ ] = (byte)(value >> 24);
        page[ offset++ ] = (byte)(value >> 16);
        page[ offset++ ] = (byte)(value >> 8);
        page[ offset++ ] = (byte)(value);
    }

    long readLong(){
        //return (((long)readInt()) << 32) | (readInt() & 0xFFFFFFFFL);
        return  ((long)(page[ offset++ ]) << 56) |
                ((long)(page[ offset++ ] & 0xFF) << 48) |
                ((long)(page[ offset++ ] & 0xFF) << 40) |
                ((long)(page[ offset++ ] & 0xFF) << 32) |
                ((long)(page[ offset++ ] & 0xFF) << 24) |
                ((page[ offset++ ] & 0xFF) << 16) |
                ((page[ offset++ ] & 0xFF) << 8) |
                ((page[ offset++ ] & 0xFF));
    }

    void writeDouble(double value){
        writeLong( Double.doubleToLongBits(value) );
    }

    double readDouble(){
        return Double.longBitsToDouble( readLong() );
    }

    void writeFloat(float value){
        writeInt( Float.floatToIntBits(value) );
    }

    float readFloat(){
        return Float.intBitsToFloat( readInt() );
    }

    void writeNumeric( MutableNumeric num){
        writeByte( num.getInternalValue().length );
        writeByte( num.getScale() );
        writeByte( num.getSignum() );
        for(int i=0; i<num.getInternalValue().length; i++){
            writeInt( num.getInternalValue()[i] );
        }
    }

    MutableNumeric readNumeric(){
        int[] value = new int[ readByte() ];
        int scale   = readByte();
        int signum  = readByte();
        for(int i=0; i<value.length; i++){
            value[i] = readInt();
        }
        return new MutableNumeric( signum, value, scale );
    }

    void writeTimestamp( long ts){
        writeLong( ts );
    }

    long readTimestamp(){
        return readLong();
    }

    void writeTime( long time){
        writeInt( (int)((time / 1000) % 86400) );
    }

    long readTime(){
        return readInt() * 1000L;
    }

    void writeDate( long date){
        writeInt( (int)(date / 86400000));
    }

    long readDate(){
        return readInt() * 86400000L;
    }

    void writeSmallDateTime( long datetime){
        writeInt( (int)(datetime / 60000));
    }

    long readSmallDateTime(){
        return readInt() * 60000L;
    }

    void writeString( String strDaten ) throws SQLException{
        writeString( strDaten, Short.MAX_VALUE, true );
    }

    void writeString( String strDaten, int lengthColumn, boolean varchar ) throws SQLException{
        char[] daten = strDaten.toCharArray();
        int length = daten.length;

        if(lengthColumn < length){
            throw SmallSQLException.create(Language.VALUE_STR_TOOLARGE);
        }
		if(varchar) lengthColumn = length;
    	int newSize = offset + 2 + 2*lengthColumn;
        if(newSize > page.length) resizePage(newSize);

        writeShort( lengthColumn );
        writeChars( daten );
        for(int i=length; i<lengthColumn; i++){
            page[ offset++ ] = ' ';
            page[ offset++ ] = 0;
        }
    }

    String readString(){
        int length = readShort() & 0xFFFF;
        return new String( readChars(length) );
    }

    void writeBytes(byte[] daten){
        int newSize = offset + daten.length;
        if(newSize > page.length) resizePage(newSize );
        System.arraycopy( daten, 0, page, offset, daten.length);
        offset += daten.length;
    }

    void writeBytes(byte[] daten, int off, int length){
        int newSize = offset + length;
        if(newSize > page.length) resizePage(newSize );
        System.arraycopy( daten, off, page, offset, length);
        offset += length;
    }

    byte[] readBytes(int length){
        byte[] daten = new byte[length];
        System.arraycopy( page, offset, daten, 0, length);
        offset += length;
        return daten;
    }

    void writeBinary( byte[] daten, int lengthColumn, boolean varBinary ) throws SQLException{
        int length = daten.length;

        if(lengthColumn < length){
        	Object params = new Object[] { new Integer(length), new Integer(lengthColumn) };
            throw SmallSQLException.create(Language.VALUE_BIN_TOOLARGE, params);
        }
        if(varBinary) lengthColumn = length;
    	int newSize = offset + 2 + lengthColumn;
        if(newSize > page.length) resizePage(newSize);

        page[ offset++ ] = (byte)(lengthColumn >> 8);
        page[ offset++ ] = (byte)(lengthColumn);
        writeBytes( daten );
        if(!varBinary){
            for(int i=length; i<lengthColumn; i++){
                page[ offset++ ] = 0;
            }
        }
    }

    byte[] readBinary(){
        int length = readShort() & 0xFFFF;
        return readBytes(length);
    }

    void writeLongBinary( byte[] daten ) throws Exception{
        StoreImpl store = table.getLobStore( ((TableStorePage)storePage).con, daten.length + 4, SQLTokenizer.LONGVARBINARY);
        store.writeInt( daten.length );
        store.writeBytes( daten );
        writeLong( store.writeFinsh(null) );
    }

    byte[] readLongBinary() throws Exception{
        long lobFilePos = readLong();
        StoreImpl store = table.getLobStore( ((TableStorePage)storePage).con, lobFilePos, SQLTokenizer.SELECT );
        return store.readBytes( store.readInt() );
    }

    void writeChars(char[] daten){
        int length = daten.length;
        int newSize = offset + 2*length;
        if(newSize > page.length) resizePage(newSize );
        for(int i=0; i<length; i++){
            char c = daten[i];
            page[ offset++ ] = (byte)(c);
            page[ offset++ ] = (byte)(c >> 8);
        }
    }

    char[] readChars(int length){
        char[] daten = new char[length];
        for(int i=0; i<length; i++){
            daten[i] = (char)((page[ offset++ ] & 0xFF) | (page[ offset++ ] << 8));
        }
        return daten;
    }

    void writeLongString(String daten) throws Exception{
        char[] chars = daten.toCharArray();
        StoreImpl store = table.getLobStore( ((TableStorePage)storePage).con, chars.length * 2L + 4, SQLTokenizer.LONGVARBINARY);
        store.writeInt( chars.length );
        store.writeChars( chars );
        writeLong( store.writeFinsh(null) );
    }

    String readLongString() throws Exception{
        long lobFilePos = readLong();
        StoreImpl store = table.getLobStore( ((TableStorePage)storePage).con, lobFilePos, SQLTokenizer.SELECT );
        if(store == null) throw SmallSQLException.create(Language.LOB_DELETED);
        return new String(store.readChars( store.readInt() ) );
    }
	

    void writeColumn(Column column ) throws Exception{
    	int newSize = offset + 25;
        if(newSize > page.length) resizePage(newSize);

        writeByte   ( column.getFlag() );
        writeString ( column.getName() );
        writeShort  ( column.getDataType() );
		writeInt    ( column.getPrecision() );
		writeByte   ( column.getScale() );
		offset += column.initAutoIncrement(storePage.raFile, filePos+offset);
		String def = column.getDefaultDefinition();
		writeBoolean( def == null );
		if(def != null)
			writeString ( column.getDefaultDefinition() );
    }

	
	/**
	 * Read a single Column description on the current file offset.
	 * @param table The parent table of the column.
	 * @param tableFormatVersion the file version of the table.
	 * @return
	 * @throws Exception
	 */
    Column readColumn(int tableFormatVersion) throws Exception{
        Column column = new Column();
        column.setFlag( readByte() );
        column.setName( readString() );
        column.setDataType( readShort() );
		int precision;
		if(tableFormatVersion == TableView.TABLE_VIEW_OLD_VERSION)
			precision = readByte();
		else
			precision = readInt();
		column.setPrecision( precision );
		column.setScale( readByte() );
		offset += column.initAutoIncrement(storePage.raFile, filePos+offset);
		if(!readBoolean()){
			String def = readString();
			column.setDefaultValue( new SQLParser().parseExpression(def), def);
		}
        return column;
    }
 
    
    void copyValueFrom( StoreImpl store, int valueOffset, int length){
		System.arraycopy( store.page, valueOffset, this.page, this.offset, length);
		this.offset += length;
    }

    
    /**
     * Here are pass only Expression instead of Objects to optimize 
     * it for native data (no object).
     * The value of offset must be correctly. 
     * @param expr the value that should be written
     * @param column a description of the target column
     * @throws Exception if any error occur like conversions or io exceptions
     */
    void writeExpression( Expression expr, Column column) throws Exception{
        boolean isNull = expr.isNull();
        if(isNull && !column.isNullable()){
            throw SmallSQLException.create(Language.VALUE_NULL_INVALID, column.getName());
        }
        int dataType = column.getDataType();
        if(isNull){
            writeBoolean(true); //true - is null
            switch(dataType){
                case SQLTokenizer.BIT:
                case SQLTokenizer.BOOLEAN:
                case SQLTokenizer.TINYINT:
                    offset++;
                    break;
                case SQLTokenizer.SMALLINT:
                case SQLTokenizer.BINARY:
                case SQLTokenizer.VARBINARY:
                case SQLTokenizer.CHAR:
                case SQLTokenizer.NCHAR:
                case SQLTokenizer.VARCHAR:
                case SQLTokenizer.NVARCHAR:
                    offset += 2;
                    break;
                case SQLTokenizer.INT:
                case SQLTokenizer.REAL:
                case SQLTokenizer.SMALLMONEY:
                case SQLTokenizer.TIME:
                case SQLTokenizer.DATE:
                case SQLTokenizer.SMALLDATETIME:
                    offset += 4;
                    break;
                case SQLTokenizer.BIGINT:
                case SQLTokenizer.FLOAT:
                case SQLTokenizer.DOUBLE:
                case SQLTokenizer.MONEY:
                case SQLTokenizer.JAVA_OBJECT:
                case SQLTokenizer.LONGVARBINARY:
                case SQLTokenizer.BLOB:
				case SQLTokenizer.CLOB:
				case SQLTokenizer.NCLOB:
                case SQLTokenizer.LONGNVARCHAR:
                case SQLTokenizer.LONGVARCHAR:
                case SQLTokenizer.TIMESTAMP:
                    offset += 8;
                    break;
                case SQLTokenizer.UNIQUEIDENTIFIER:
                    offset += 16;
                    break;
                case SQLTokenizer.NUMERIC:
                case SQLTokenizer.DECIMAL:
                    offset += 3;
                    break;
                default: throw new Error();
            }
            return;
        }
        writeBoolean(false); // false - is not null
       	column.setNewAutoIncrementValue(expr);
        switch(dataType){
            case SQLTokenizer.BIT:
            case SQLTokenizer.BOOLEAN:
                    writeBoolean( expr.getBoolean() );
                    break;
            case SQLTokenizer.BINARY:
            case SQLTokenizer.VARBINARY:
                    writeBinary( expr.getBytes(), column.getPrecision(), dataType != SQLTokenizer.BINARY );
                    break;
            case SQLTokenizer.TINYINT:
                    writeByte( expr.getInt() );
                    break;
            case SQLTokenizer.SMALLINT:
                    writeShort( expr.getInt() );
                    break;
            case SQLTokenizer.INT:
                    writeInt( expr.getInt() );
                    break;
            case SQLTokenizer.BIGINT:
                    writeLong( expr.getLong() );
                    break;
            case SQLTokenizer.REAL:
                    writeFloat( expr.getFloat() );
                    break;
            case SQLTokenizer.FLOAT:
            case SQLTokenizer.DOUBLE:
                    writeDouble( expr.getDouble() );
                    break;
            case SQLTokenizer.MONEY:
                    writeLong( expr.getMoney() );
                    break;
            case SQLTokenizer.SMALLMONEY:
                    writeInt( (int)expr.getMoney() );
                    break;
            case SQLTokenizer.NUMERIC:
            case SQLTokenizer.DECIMAL:
            		MutableNumeric numeric = expr.getNumeric();
            		numeric.setScale( column.getScale() );
                    writeNumeric( numeric );
                    break;
            case SQLTokenizer.CHAR:
            case SQLTokenizer.NCHAR:
                    writeString( expr.getString(), column.getDisplaySize(), false );
                    break;
            case SQLTokenizer.VARCHAR:
            case SQLTokenizer.NVARCHAR:
                    writeString( expr.getString(), column.getDisplaySize(), true );
                    break;
			case SQLTokenizer.CLOB:
			case SQLTokenizer.NCLOB:
            case SQLTokenizer.LONGNVARCHAR:
            case SQLTokenizer.LONGVARCHAR:
                    writeLongString( expr.getString() );
                    break;
            case SQLTokenizer.JAVA_OBJECT:
                    // FIXME a MemoryStream can be faster because there are no additional array copy
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    ObjectOutputStream oos = new ObjectOutputStream(baos);
                    oos.writeObject( expr.getObject() );
                    writeLongBinary( baos.toByteArray() );
                    break;
            case SQLTokenizer.LONGVARBINARY:
            case SQLTokenizer.BLOB:
                    writeLongBinary( expr.getBytes() );
                    break;
            case SQLTokenizer.TIMESTAMP:
                    writeTimestamp( expr.getLong() );
                    break;
            case SQLTokenizer.TIME:
                    writeTime( expr.getLong() );
                    break;
            case SQLTokenizer.DATE:
                    writeDate( expr.getLong() );
                    break;
            case SQLTokenizer.SMALLDATETIME:
                    writeSmallDateTime( expr.getLong() );
                    break;
            case SQLTokenizer.UNIQUEIDENTIFIER:
					switch(expr.getDataType()){
					case SQLTokenizer.UNIQUEIDENTIFIER:
					case SQLTokenizer.BINARY:
					case SQLTokenizer.VARBINARY:
					case SQLTokenizer.LONGVARBINARY:
					case SQLTokenizer.BLOB:
						byte[] bytes = expr.getBytes();
                        if(bytes.length != 16) throw SmallSQLException.create(Language.BYTEARR_INVALID_SIZE, String.valueOf(bytes.length));
                        writeBytes( bytes );
					default:
                        writeBytes( Utils.unique2bytes(expr.getString()) );
					}
                    break;
            default: throw new Error(String.valueOf(column.getDataType()));
        }
    }

    @Override
    boolean isNull(int valueOffset){
        return page[ valueOffset ] != 0;
    }

    
    /**
     * It used for ResultSet.getBoolean()
     */
    @Override
    boolean getBoolean(int valueOffset, int dataType) throws Exception{
        this.offset = valueOffset;
        if(readBoolean()) return false;
        switch(dataType){
            case SQLTokenizer.BIT:
            case SQLTokenizer.BOOLEAN:
                    return readBoolean();
            case SQLTokenizer.BINARY:
            case SQLTokenizer.VARBINARY:
                    return Utils.bytes2int( readBinary() ) != 0;
            case SQLTokenizer.TINYINT:
                    return readUnsignedByte() != 0;
            case SQLTokenizer.SMALLINT:
                    return readShort() != 0;
            case SQLTokenizer.INT:
                    return readInt() != 0;
            case SQLTokenizer.BIGINT:
                    return readLong() != 0;
            case SQLTokenizer.REAL:
                    return readFloat() != 0;
            case SQLTokenizer.FLOAT:
            case SQLTokenizer.DOUBLE:
                    return readDouble() != 0;
            case SQLTokenizer.MONEY:
                    return readLong() != 0;
            case SQLTokenizer.SMALLMONEY:
                    return readInt() != 0;
            case SQLTokenizer.NUMERIC:
            case SQLTokenizer.DECIMAL:
                    return readNumeric().getSignum() != 0;
            case SQLTokenizer.CHAR:
            case SQLTokenizer.NCHAR:
            case SQLTokenizer.VARCHAR:
            case SQLTokenizer.NVARCHAR:
                    return Utils.string2boolean( readString() );
			case SQLTokenizer.CLOB:
			case SQLTokenizer.NCLOB:
            case SQLTokenizer.LONGNVARCHAR:
            case SQLTokenizer.LONGVARCHAR:
                    return Utils.string2boolean( readLongString() );
            case SQLTokenizer.JAVA_OBJECT:
                    ByteArrayInputStream bais = new ByteArrayInputStream(readLongBinary());
                    ObjectInputStream ois = new ObjectInputStream(bais);
                    return Utils.string2boolean( ois.readObject().toString() );
            case SQLTokenizer.LONGVARBINARY:
            case SQLTokenizer.BLOB:
                    return Utils.bytes2int( readLongBinary() ) != 0;
			case SQLTokenizer.TIMESTAMP:
					return readTimestamp() != 0;
			case SQLTokenizer.TIME:
					return readTime() != 0;
			case SQLTokenizer.DATE:
					return readDate() != 0;
			case SQLTokenizer.SMALLDATETIME:
					return readSmallDateTime() != 0;
            case SQLTokenizer.UNIQUEIDENTIFIER:
                return false;
			default: 
				throw SmallSQLException.create(Language.VALUE_CANT_CONVERT, new Object[] { SQLTokenizer.getKeyWord(dataType), "BOOLEAN" });
        }
    }

    
    /**
     * It used for ResultSet.getInt()
     */
    @Override
    int getInt(int valueOffset, int dataType) throws Exception{
        this.offset = valueOffset;
        if(readBoolean()) return 0;
        switch(dataType){
            case SQLTokenizer.BIT:
            case SQLTokenizer.BOOLEAN:
                    return readBoolean() ? 1 : 0;
            case SQLTokenizer.BINARY:
            case SQLTokenizer.VARBINARY:
                    return Utils.bytes2int( readBinary() );
            case SQLTokenizer.TINYINT:
                    return readUnsignedByte();
            case SQLTokenizer.SMALLINT:
                    return readShort();
            case SQLTokenizer.INT:
                    return readInt();
            case SQLTokenizer.BIGINT:
                    return (int)readLong();
            case SQLTokenizer.REAL:
                    return (int)readFloat();
            case SQLTokenizer.FLOAT:
            case SQLTokenizer.DOUBLE:
                    return (int)readDouble();
            case SQLTokenizer.MONEY:
            		long longValue = readLong() / 10000;
            		return Utils.money2int(longValue);
            case SQLTokenizer.SMALLMONEY:
                    return readInt() / 10000;
            case SQLTokenizer.NUMERIC:
            case SQLTokenizer.DECIMAL:
                    return readNumeric().intValue();
            case SQLTokenizer.CHAR:
            case SQLTokenizer.NCHAR:
            case SQLTokenizer.VARCHAR:
            case SQLTokenizer.NVARCHAR:
                    return Integer.parseInt( readString() );
			case SQLTokenizer.CLOB:
			case SQLTokenizer.NCLOB:
            case SQLTokenizer.LONGNVARCHAR:
            case SQLTokenizer.LONGVARCHAR:
                    return Integer.parseInt( readLongString() );
            case SQLTokenizer.JAVA_OBJECT:
                    ByteArrayInputStream bais = new ByteArrayInputStream(readLongBinary());
                    ObjectInputStream ois = new ObjectInputStream(bais);
                    return ExpressionValue.getInt(ois.readObject().toString(), SQLTokenizer.VARCHAR);
            case SQLTokenizer.LONGVARBINARY:
            case SQLTokenizer.BLOB:
                    return Utils.bytes2int( readLongBinary() );
			case SQLTokenizer.TIMESTAMP:
					return (int)readTimestamp();
			case SQLTokenizer.TIME:
					return (int)readTime();
			case SQLTokenizer.DATE:
					return (int)readDate();
			case SQLTokenizer.SMALLDATETIME:
					return (int)readSmallDateTime();
			default:
				throw SmallSQLException.create(Language.VALUE_CANT_CONVERT, new Object[] { SQLTokenizer.getKeyWord(dataType), "INT" });
        }
    }

    
    /**
     * It used for ResultSet.getLong()
     */
    @Override
    long getLong(int valueOffset, int dataType) throws Exception{
        this.offset = valueOffset;
        if(readBoolean()) return 0;
        switch(dataType){
            case SQLTokenizer.BIT:
            case SQLTokenizer.BOOLEAN:
                    return readBoolean() ? 1 : 0;
            case SQLTokenizer.BINARY:
            case SQLTokenizer.VARBINARY:
                    return Utils.bytes2long( readBinary() );
            case SQLTokenizer.TINYINT:
                    return readUnsignedByte();
            case SQLTokenizer.SMALLINT:
                    return readShort();
            case SQLTokenizer.INT:
                    return readInt();
            case SQLTokenizer.BIGINT:
                    return readLong();
            case SQLTokenizer.REAL:
                    return (long)readFloat();
            case SQLTokenizer.FLOAT:
            case SQLTokenizer.DOUBLE:
                    return (long)readDouble();
            case SQLTokenizer.MONEY:
                    return readLong() / 10000;
            case SQLTokenizer.SMALLMONEY:
                    return readInt() / 10000;
            case SQLTokenizer.NUMERIC:
            case SQLTokenizer.DECIMAL:
                    return readNumeric().longValue();
            case SQLTokenizer.CHAR:
            case SQLTokenizer.NCHAR:
            case SQLTokenizer.VARCHAR:
            case SQLTokenizer.NVARCHAR:
                    return Long.parseLong( readString() );
			case SQLTokenizer.CLOB:
			case SQLTokenizer.NCLOB:
            case SQLTokenizer.LONGNVARCHAR:
            case SQLTokenizer.LONGVARCHAR:
                    return Long.parseLong( readLongString() );
            case SQLTokenizer.JAVA_OBJECT:
                    ByteArrayInputStream bais = new ByteArrayInputStream(readLongBinary());
                    ObjectInputStream ois = new ObjectInputStream(bais);
                    return ExpressionValue.getLong( ois.readObject().toString(), SQLTokenizer.VARCHAR );
            case SQLTokenizer.LONGVARBINARY:
            case SQLTokenizer.BLOB:
                    return Utils.bytes2long( readLongBinary() );
			case SQLTokenizer.TIMESTAMP:
					return readTimestamp();
			case SQLTokenizer.TIME:
					return readTime();
			case SQLTokenizer.DATE:
					return readDate();
			case SQLTokenizer.SMALLDATETIME:
					return readSmallDateTime();
			default:
				throw SmallSQLException.create(Language.VALUE_CANT_CONVERT, new Object[] { SQLTokenizer.getKeyWord(dataType), "BIGINT" });
        }
    }

    
    /**
     * It used for ResultSet.getFloat()
     */
    @Override
    float getFloat(int valueOffset, int dataType) throws Exception{
        this.offset = valueOffset;
        if(readBoolean()) return 0;
        switch(dataType){
            case SQLTokenizer.BIT:
            case SQLTokenizer.BOOLEAN:
                    return readBoolean() ? 1 : 0;
            case SQLTokenizer.BINARY:
            case SQLTokenizer.VARBINARY:
                    return Utils.bytes2float( readBinary() );
            case SQLTokenizer.TINYINT:
                    return readUnsignedByte();
            case SQLTokenizer.SMALLINT:
                    return readShort();
            case SQLTokenizer.INT:
                    return readInt();
            case SQLTokenizer.BIGINT:
                    return readLong();
            case SQLTokenizer.REAL:
                    return readFloat();
            case SQLTokenizer.FLOAT:
            case SQLTokenizer.DOUBLE:
                    return (float)readDouble();
            case SQLTokenizer.MONEY:
                    return readLong() / (float)10000.0;
            case SQLTokenizer.SMALLMONEY:
                    return readInt() / (float)10000.0;
            case SQLTokenizer.NUMERIC:
            case SQLTokenizer.DECIMAL:
                    return readNumeric().floatValue();
            case SQLTokenizer.CHAR:
            case SQLTokenizer.NCHAR:
            case SQLTokenizer.VARCHAR:
            case SQLTokenizer.NVARCHAR:
                    return Float.parseFloat( readString() );
			case SQLTokenizer.CLOB:
			case SQLTokenizer.NCLOB:
            case SQLTokenizer.LONGNVARCHAR:
            case SQLTokenizer.LONGVARCHAR:
                    return Float.parseFloat( readLongString() );
            case SQLTokenizer.JAVA_OBJECT:
                    ByteArrayInputStream bais = new ByteArrayInputStream(readLongBinary());
                    ObjectInputStream ois = new ObjectInputStream(bais);
                    return Float.parseFloat( ois.readObject().toString() );
            case SQLTokenizer.LONGVARBINARY:
            case SQLTokenizer.BLOB:
                    return Utils.bytes2float( readLongBinary() );
			case SQLTokenizer.TIMESTAMP:
					return readTimestamp();
			case SQLTokenizer.TIME:
					return readTime();
			case SQLTokenizer.DATE:
					return readDate();
			case SQLTokenizer.SMALLDATETIME:
					return readSmallDateTime();
			default:
				throw SmallSQLException.create(Language.VALUE_CANT_CONVERT, new Object[] { SQLTokenizer.getKeyWord(dataType), "REAL" });
        }
    }

    
    /**
     * It used for ResultSet.getDouble()
     */
    @Override
    double getDouble(int valueOffset, int dataType) throws Exception{
        this.offset = valueOffset;
        if(readBoolean()) return 0;
        switch(dataType){
            case SQLTokenizer.BIT:
            case SQLTokenizer.BOOLEAN:
                    return readBoolean() ? 1 : 0;
            case SQLTokenizer.BINARY:
            case SQLTokenizer.VARBINARY:
                    return Utils.bytes2double( readBinary() );
            case SQLTokenizer.TINYINT:
                    return readUnsignedByte();
            case SQLTokenizer.SMALLINT:
                    return readShort();
            case SQLTokenizer.INT:
                    return readInt();
            case SQLTokenizer.BIGINT:
                    return readLong();
            case SQLTokenizer.REAL:
                    return readFloat();
            case SQLTokenizer.FLOAT:
            case SQLTokenizer.DOUBLE:
                    return readDouble();
            case SQLTokenizer.MONEY:
                    return readLong() / 10000.0;
            case SQLTokenizer.SMALLMONEY:
                    return readInt() / 10000.0;
            case SQLTokenizer.NUMERIC:
            case SQLTokenizer.DECIMAL:
                    return readNumeric().doubleValue();
            case SQLTokenizer.CHAR:
            case SQLTokenizer.NCHAR:
            case SQLTokenizer.VARCHAR:
            case SQLTokenizer.NVARCHAR:
                    return Double.parseDouble( readString() );
			case SQLTokenizer.CLOB:
			case SQLTokenizer.NCLOB:
            case SQLTokenizer.LONGNVARCHAR:
            case SQLTokenizer.LONGVARCHAR:
                    return Double.parseDouble( readLongString() );
            case SQLTokenizer.JAVA_OBJECT:
                    ByteArrayInputStream bais = new ByteArrayInputStream(readLongBinary());
                    ObjectInputStream ois = new ObjectInputStream(bais);
                    return Double.parseDouble( ois.readObject().toString() );
            case SQLTokenizer.LONGVARBINARY:
            case SQLTokenizer.BLOB:
                    return Utils.bytes2double( readLongBinary() );
			case SQLTokenizer.TIMESTAMP:
					return readTimestamp();
			case SQLTokenizer.TIME:
					return readTime();
			case SQLTokenizer.DATE:
					return readDate();
			case SQLTokenizer.SMALLDATETIME:
					return readSmallDateTime();
			default:
				throw SmallSQLException.create(Language.VALUE_CANT_CONVERT, new Object[] { SQLTokenizer.getKeyWord(dataType), "NUMERIC" });
        }
    }

    // is used for faster calculation
    @Override
    long getMoney( int valueOffset, int dataType) throws Exception{
        this.offset = valueOffset;
        if(readBoolean()) return 0;
        switch(dataType){
            case SQLTokenizer.BIT:
            case SQLTokenizer.BOOLEAN:
                    return readBoolean() ? 10000 : 0;
            case SQLTokenizer.BINARY:
            case SQLTokenizer.VARBINARY:
                    return (long)(Utils.bytes2double( readBinary() ) * 10000L);
            case SQLTokenizer.TINYINT:
                    return readUnsignedByte() * 10000L;
            case SQLTokenizer.SMALLINT:
                    return readShort() * 10000L;
            case SQLTokenizer.INT:
                    return readInt() * 10000L;
            case SQLTokenizer.BIGINT:
                    return readLong() * 10000L;
            case SQLTokenizer.REAL:
                    return (long)(readFloat() * 10000L);
            case SQLTokenizer.FLOAT:
            case SQLTokenizer.DOUBLE:
                    return (long)(readDouble() * 10000L);
            case SQLTokenizer.MONEY:
                    return readLong();
            case SQLTokenizer.SMALLMONEY:
                    return readInt();
            case SQLTokenizer.NUMERIC:
            case SQLTokenizer.DECIMAL:
                    return (long)(readNumeric().doubleValue() * 10000L);
            case SQLTokenizer.CHAR:
            case SQLTokenizer.NCHAR:
            case SQLTokenizer.VARCHAR:
            case SQLTokenizer.NVARCHAR:
                    return Money.parseMoney( readString() );
			case SQLTokenizer.CLOB:
			case SQLTokenizer.NCLOB:
            case SQLTokenizer.LONGNVARCHAR:
            case SQLTokenizer.LONGVARCHAR:
                    return Money.parseMoney( readLongString() );
            case SQLTokenizer.JAVA_OBJECT:
                    ByteArrayInputStream bais = new ByteArrayInputStream(readLongBinary());
                    ObjectInputStream ois = new ObjectInputStream(bais);
                    return Money.parseMoney( ois.readObject().toString() );
            case SQLTokenizer.LONGVARBINARY:
            case SQLTokenizer.BLOB:
                    return (long)(Utils.bytes2double( readLongBinary() ) * 10000L);
            case SQLTokenizer.TIMESTAMP:
            case SQLTokenizer.TIME:
            case SQLTokenizer.DATE:
            case SQLTokenizer.SMALLDATETIME:
					throw SmallSQLException.create(Language.VALUE_CANT_CONVERT, new Object[] { SQLTokenizer.getKeyWord(dataType), "MONEY" });
            default: throw new Error();
        }
    }

    
    /**
     * It used for ResultSet.getBigDecimal()
     */
    @Override
    MutableNumeric getNumeric(int valueOffset, int dataType) throws Exception{
        this.offset = valueOffset;
        if(readBoolean()) return null;
        switch(dataType){
            case SQLTokenizer.BIT:
            case SQLTokenizer.BOOLEAN:
                    return readBoolean() ? new MutableNumeric(1) : new MutableNumeric(0);
            case SQLTokenizer.BINARY:
            case SQLTokenizer.VARBINARY:
                    return new MutableNumeric(Utils.bytes2double( readBinary() ));
            case SQLTokenizer.TINYINT:
                    return new MutableNumeric(readUnsignedByte());
            case SQLTokenizer.SMALLINT:
                    return new MutableNumeric(readShort());
            case SQLTokenizer.INT:
                    return new MutableNumeric(readInt());
            case SQLTokenizer.BIGINT:
                    return new MutableNumeric(readLong());
            case SQLTokenizer.REAL:
                    return new MutableNumeric(readFloat());
            case SQLTokenizer.FLOAT:
            case SQLTokenizer.DOUBLE:
                    return new MutableNumeric(readDouble());
            case SQLTokenizer.MONEY:
                    return new MutableNumeric( readLong(), 4);
            case SQLTokenizer.SMALLMONEY:
                    return new MutableNumeric( readInt(), 4);
            case SQLTokenizer.NUMERIC:
            case SQLTokenizer.DECIMAL:
                    return readNumeric();
            case SQLTokenizer.CHAR:
            case SQLTokenizer.NCHAR:
            case SQLTokenizer.VARCHAR:
            case SQLTokenizer.NVARCHAR:
                    return new MutableNumeric( readString() );
			case SQLTokenizer.CLOB:
			case SQLTokenizer.NCLOB:
            case SQLTokenizer.LONGNVARCHAR:
            case SQLTokenizer.LONGVARCHAR:
                    return new MutableNumeric( readLongString() );
            case SQLTokenizer.JAVA_OBJECT:
                    ByteArrayInputStream bais = new ByteArrayInputStream(readLongBinary());
                    ObjectInputStream ois = new ObjectInputStream(bais);
                    return new MutableNumeric( ois.readObject().toString() );
            case SQLTokenizer.LONGVARBINARY:
            case SQLTokenizer.BLOB:
                    return new MutableNumeric( Utils.bytes2double( readLongBinary() ) );
            case SQLTokenizer.TIMESTAMP:
            case SQLTokenizer.TIME:
            case SQLTokenizer.DATE:
            case SQLTokenizer.SMALLDATETIME:
                    throw SmallSQLException.create(Language.VALUE_CANT_CONVERT, new Object[] { SQLTokenizer.getKeyWord(dataType), "NUMERIC" });
            // SAVER: check if this should be a kind of "type not recognized" exception.
            default: throw new Error();
        }
    }


    /**
     * It used for ResultSet.getObject()
     */
    @Override
    Object getObject(int valueOffset, int dataType) throws Exception{
        this.offset = valueOffset;
        if(readBoolean()) return null;
        switch(dataType){
            case SQLTokenizer.BIT:
            case SQLTokenizer.BOOLEAN:
                    return readBoolean() ? Boolean.TRUE : Boolean.FALSE;
            case SQLTokenizer.BINARY:
            case SQLTokenizer.VARBINARY:
                    return readBinary();
            case SQLTokenizer.TINYINT:
                    return Utils.getInteger( readUnsignedByte() );
            case SQLTokenizer.SMALLINT:
                    return Utils.getInteger( readShort() );
            case SQLTokenizer.INT:
                    return Utils.getInteger(readInt());
            case SQLTokenizer.BIGINT:
                    return new Long(readLong());
            case SQLTokenizer.REAL:
                    return new Float( readFloat() );
            case SQLTokenizer.FLOAT:
            case SQLTokenizer.DOUBLE:
                    return new Double( readDouble() );
            case SQLTokenizer.MONEY:
                    return Money.createFromUnscaledValue(readLong());
            case SQLTokenizer.SMALLMONEY:
                    return Money.createFromUnscaledValue(readInt());
            case SQLTokenizer.NUMERIC:
            case SQLTokenizer.DECIMAL:
                    return readNumeric();
            case SQLTokenizer.CHAR:
            case SQLTokenizer.NCHAR:
            case SQLTokenizer.VARCHAR:
            case SQLTokenizer.NVARCHAR:
                    return readString();
			case SQLTokenizer.CLOB:
			case SQLTokenizer.NCLOB:
            case SQLTokenizer.LONGNVARCHAR:
            case SQLTokenizer.LONGVARCHAR:
                    return readLongString();
            case SQLTokenizer.JAVA_OBJECT:
                    ByteArrayInputStream bais = new ByteArrayInputStream(readLongBinary());
                    ObjectInputStream ois = new ObjectInputStream(bais);
                    return ois.readObject();
            case SQLTokenizer.LONGVARBINARY:
            case SQLTokenizer.BLOB:
                    return readLongBinary();
            case SQLTokenizer.TIMESTAMP:
                    return new DateTime( readTimestamp(), SQLTokenizer.TIMESTAMP );
            case SQLTokenizer.TIME:
                    return new DateTime( readTime(), SQLTokenizer.TIME );
            case SQLTokenizer.DATE:
                    return new DateTime( readDate(), SQLTokenizer.DATE );
            case SQLTokenizer.SMALLDATETIME:
                    return new DateTime( readSmallDateTime(), SQLTokenizer.TIMESTAMP );
            case SQLTokenizer.UNIQUEIDENTIFIER:
                    return Utils.bytes2unique( page, this.offset);
            default: throw new Error();
        }
    }

    
    /**
     * It used for ResultSet.getString()
     */
    @Override
    String getString( int valueOffset, int dataType) throws Exception{
        this.offset = valueOffset;
        if(readBoolean()) return null;
        switch(dataType){
            case SQLTokenizer.BIT:
                    return readBoolean() ? "1" : "0";
            case SQLTokenizer.BOOLEAN:
                    return String.valueOf( readBoolean() );
            case SQLTokenizer.BINARY:
            case SQLTokenizer.VARBINARY:
                    return Utils.bytes2hex( readBinary() );
            case SQLTokenizer.TINYINT:
                    return String.valueOf( readUnsignedByte() );
            case SQLTokenizer.SMALLINT:
                    return String.valueOf( readShort() );
            case SQLTokenizer.INT:
                    return String.valueOf( readInt() );
            case SQLTokenizer.BIGINT:
                    return String.valueOf( readLong() );
            case SQLTokenizer.REAL:
                    return String.valueOf( readFloat() );
            case SQLTokenizer.FLOAT:
            case SQLTokenizer.DOUBLE:
                    return String.valueOf( readDouble() );
            case SQLTokenizer.MONEY:
                    return Money.createFromUnscaledValue( readLong() ).toString();
            case SQLTokenizer.SMALLMONEY:
                    return Money.createFromUnscaledValue( readInt() ).toString();
            case SQLTokenizer.NUMERIC:
            case SQLTokenizer.DECIMAL:
                    return readNumeric().toString();
            case SQLTokenizer.CHAR:
            case SQLTokenizer.NCHAR:
            case SQLTokenizer.VARCHAR:
            case SQLTokenizer.NVARCHAR:
                    return readString();
			case SQLTokenizer.CLOB:
			case SQLTokenizer.NCLOB:
            case SQLTokenizer.LONGNVARCHAR:
            case SQLTokenizer.LONGVARCHAR:
                    return readLongString();
            case SQLTokenizer.JAVA_OBJECT:
                    ByteArrayInputStream bais = new ByteArrayInputStream(readLongBinary());
                    ObjectInputStream ois = new ObjectInputStream(bais);
                    return ois.readObject().toString();
            case SQLTokenizer.LONGVARBINARY:
            case SQLTokenizer.BLOB:
                    return Utils.bytes2hex( readLongBinary() );
            case SQLTokenizer.TIMESTAMP:
                    return new DateTime( readTimestamp(), SQLTokenizer.TIMESTAMP ).toString();
            case SQLTokenizer.TIME:
                    return new DateTime( readTime(), SQLTokenizer.TIME ).toString();
            case SQLTokenizer.DATE:
                    return new DateTime( readDate(), SQLTokenizer.DATE ).toString();
            case SQLTokenizer.SMALLDATETIME:
                    return new DateTime( readSmallDateTime(), SQLTokenizer.TIMESTAMP ).toString();
            case SQLTokenizer.UNIQUEIDENTIFIER:
                    return Utils.bytes2unique( page, this.offset);
            default: throw new Error();
        }
    }

    
    /**
     * It used for ResultSet.getBytes()
     */
    @Override
    byte[] getBytes(int valueOffset, int dataType) throws Exception{
        this.offset = valueOffset;
        if(readBoolean()) return null;
        switch(dataType){
            case SQLTokenizer.BINARY:
            case SQLTokenizer.VARBINARY:
                    return readBinary();
            case SQLTokenizer.TINYINT:
            case SQLTokenizer.BIT:
            case SQLTokenizer.BOOLEAN:
                    byte[] bytes = new byte[1];
                    System.arraycopy( page, valueOffset, bytes, 0, bytes.length);
                    return bytes;
            case SQLTokenizer.SMALLINT:
                    bytes = new byte[2];
                    System.arraycopy( page, valueOffset, bytes, 0, bytes.length);
                    return bytes;
            case SQLTokenizer.INT:
            case SQLTokenizer.REAL:
            case SQLTokenizer.SMALLMONEY:
            case SQLTokenizer.TIME:
            case SQLTokenizer.DATE:
            case SQLTokenizer.SMALLDATETIME:
                    bytes = new byte[4];
                    System.arraycopy( page, valueOffset, bytes, 0, bytes.length);
                    return bytes;
            case SQLTokenizer.BIGINT:
            case SQLTokenizer.FLOAT:
            case SQLTokenizer.DOUBLE:
            case SQLTokenizer.MONEY:
            case SQLTokenizer.TIMESTAMP:
                    bytes = new byte[8];
                    System.arraycopy( page, valueOffset, bytes, 0, bytes.length);
                    return bytes;
            case SQLTokenizer.NUMERIC:
            case SQLTokenizer.DECIMAL:
                    return readNumeric().toByteArray();
            case SQLTokenizer.CHAR:
            case SQLTokenizer.NCHAR:
            case SQLTokenizer.VARCHAR:
            case SQLTokenizer.NVARCHAR:
                    return readString().getBytes();
			case SQLTokenizer.CLOB:
			case SQLTokenizer.NCLOB:
            case SQLTokenizer.LONGNVARCHAR:
            case SQLTokenizer.LONGVARCHAR:
                    return readLongString().getBytes();
            case SQLTokenizer.JAVA_OBJECT:
            case SQLTokenizer.LONGVARBINARY:
            case SQLTokenizer.BLOB:
                    return readLongBinary();
            case SQLTokenizer.UNIQUEIDENTIFIER:
                    bytes = new byte[16];
                    System.arraycopy( page, valueOffset, bytes, 0, bytes.length);
                    return bytes;
            default: throw new Error();
        }
    }

    @Override
    void scanObjectOffsets( int[] offsets, int dataTypes[] ){
        offset = PAGE_CONTROL_SIZE;
        for(int i=0; i<offsets.length; i++){
            offsets[i] = offset;
            boolean isNull = readBoolean(); // for isNull
            switch(dataTypes[i]){
                case SQLTokenizer.BIT:
                case SQLTokenizer.BOOLEAN:
                case SQLTokenizer.TINYINT:
                    offset++;
                    break;
                case SQLTokenizer.SMALLINT:
                    offset += 2;
                    break;
                case SQLTokenizer.INT:
                case SQLTokenizer.REAL:
                case SQLTokenizer.SMALLMONEY:
                case SQLTokenizer.TIME:
                case SQLTokenizer.DATE:
                case SQLTokenizer.SMALLDATETIME:
                    offset += 4;
                    break;
                case SQLTokenizer.BIGINT:
                case SQLTokenizer.FLOAT:
                case SQLTokenizer.DOUBLE:
                case SQLTokenizer.MONEY:
                case SQLTokenizer.JAVA_OBJECT:
                case SQLTokenizer.LONGVARBINARY:
                case SQLTokenizer.BLOB:
				case SQLTokenizer.CLOB:
				case SQLTokenizer.NCLOB:
                case SQLTokenizer.LONGNVARCHAR:
                case SQLTokenizer.LONGVARCHAR:
                case SQLTokenizer.TIMESTAMP:
                    offset += 8;
                    break;
                case SQLTokenizer.BINARY:
                case SQLTokenizer.VARBINARY:
                    int count = readShort() & 0xFFFF;
                    if(!isNull) offset += count;  // not use += directly because readShort change the offset
                    break;
                case SQLTokenizer.NUMERIC:
                case SQLTokenizer.DECIMAL:
                    count = readByte();
					offset += 2;
					if(!isNull) offset += count*4;
                    break;
                case SQLTokenizer.CHAR:
                case SQLTokenizer.NCHAR:
                case SQLTokenizer.VARCHAR:
                case SQLTokenizer.NVARCHAR:
                    count = readShort() & 0xFFFF;
                    if(!isNull) offset += count << 1; // not use += directly because readShort change the offset
                    break;
                case SQLTokenizer.UNIQUEIDENTIFIER:
                    offset += 16;
                    break;
                default: throw new Error(String.valueOf( dataTypes[i] ) );
            }
        }
    }

	@Override
    void deleteRow(SSConnection con) throws SQLException{
		status = DELETED;
		type   = SQLTokenizer.DELETE;
		createWriteLock();
		writeFinsh(con);
	}
	
	
	StorePageLink getLink(){
		return ((TableStorePageInsert)storePage).getLink();
	}
    
    /**
     * If this store is a ghost because it was rollback.
     */
    boolean isRollback(){
        return storePage.raFile == null;
    }
}