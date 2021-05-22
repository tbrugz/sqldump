package tbrugz.sqldump.sqlrun.importers;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import tbrugz.sqldump.sqlrun.def.Constants;
import tbrugz.sqldump.sqlrun.importers.AbstractImporter.IOCounter;
import tbrugz.sqldump.util.Utils;

public class XlsImporter extends BaseImporter {

	static final Log log = LogFactory.getLog(XlsImporter.class);

	static final String SUFFIX_SHEET_NUMBER = ".sheet-number";
	static final String SUFFIX_SHEET_NAME = ".sheet-name";
	static final String SUFFIX_1ST_LINE_IS_HEADER = ".1st-line-is-header";
	static final String SUFFIX_1ST_LINE_AS_COLUMN_NAMES = ".1st-line-as-column-names";
	static final String SUFFIX_DO_CREATE_TABLE = ".do-create-table";
	
	String importFile;
	String sheetName;
	Integer sheetNumber;
	long linesToSkip = 0;
	boolean hasHeaderLine = true;
	boolean use1stLineAsColNames = false;
	boolean doCreateTable = false;
	boolean ignoreRowWithWrongNumberOfColumns = true; //XXX: add prop?
	
	static final String[] XLS_AUX_SUFFIXES = {
		SUFFIX_SHEET_NUMBER, SUFFIX_SHEET_NAME, SUFFIX_1ST_LINE_IS_HEADER, SUFFIX_1ST_LINE_AS_COLUMN_NAMES, SUFFIX_DO_CREATE_TABLE
	};
	
	@Override
	public List<String> getAuxSuffixes() {
		return Arrays.asList(XLS_AUX_SUFFIXES);
	}
	
	@Override
	public void setProperties(Properties prop) {
		super.setProperties(prop);
		
		importFile = prop.getProperty(Constants.PREFIX_EXEC+execId+AbstractImporter.SUFFIX_IMPORTFILE);
		sheetName = prop.getProperty(Constants.PREFIX_EXEC+execId+SUFFIX_SHEET_NAME);
		Long lSheetNumber = Utils.getPropLong(prop, Constants.PREFIX_EXEC+execId+SUFFIX_SHEET_NUMBER);
		if(lSheetNumber!=null) {
			sheetNumber = lSheetNumber.intValue();
		}
		/*if(sheetName==null && sheetNumber==null) {
			log.info("no sheet number (suffix '"+ SUFFIX_SHEET_NUMBER + "') nor sheet name (suffix '" + SUFFIX_SHEET_NAME + "') defined - will use 1st sheet");
		}*/
		linesToSkip = Utils.getPropLong(prop, Constants.PREFIX_EXEC+execId+AbstractImporter.SUFFIX_SKIP_N, linesToSkip);
		hasHeaderLine = Utils.getPropBool(prop, Constants.PREFIX_EXEC+execId+SUFFIX_1ST_LINE_IS_HEADER, hasHeaderLine);
		use1stLineAsColNames = Utils.getPropBool(prop, Constants.PREFIX_EXEC+execId+SUFFIX_1ST_LINE_AS_COLUMN_NAMES, use1stLineAsColNames);
		doCreateTable = Utils.getPropBool(prop, Constants.PREFIX_EXEC+execId+SUFFIX_DO_CREATE_TABLE, doCreateTable);

		if(!hasHeaderLine && use1stLineAsColNames) {
			log.warn("using '"+SUFFIX_1ST_LINE_AS_COLUMN_NAMES+"' without '"+SUFFIX_1ST_LINE_IS_HEADER+"' is invalid - will be ignored");
		}
	}

	@Override
	public long importData() throws SQLException, InterruptedException, IOException {
		if(importFile==null) {
			throw new IllegalStateException("null importFile");
		}
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(importFile);
			return importStream(fis);
		}
		finally {
			fis.close();
		}
	}
		
	@Override
	public long importStream(InputStream is) throws SQLException, InterruptedException, IOException {
		IOCounter counter = new IOCounter();
		try {
			Workbook wb = WorkbookFactory.create(is);
			//log.info("number-of-sheets: "+wb.getNumberOfSheets());
			Sheet sheet = null;
			if(sheetNumber!=null) {
				sheet = wb.getSheetAt(sheetNumber);
			}
			else if(sheetName!=null) {
				sheet = wb.getSheet(sheetName);
			}
			else {
				log.info("no sheet number (suffix '"+ SUFFIX_SHEET_NUMBER + "') nor sheet name (suffix '" + SUFFIX_SHEET_NAME + "') defined - using 1st sheet"
						+" [#sheets = "+wb.getNumberOfSheets()+"]");
				sheet = wb.getSheetAt(0);
			}
			
			boolean is1stLine = true;
			PreparedStatement stmt = null;
			boolean tableCreated = false;
			
			for (Row row : sheet) {
				counter.input++;
				if(counter.input<=linesToSkip) { continue; }
				
				List<Object> parts = new ArrayList<Object>();
				try {
				
				for (Cell cell : row) {
					parts.add(getValue(cell));
				}

				//log.info(is1stLine+";"+hasHeaderLine+";"+use1stLineAsColNames+"\nnames = "+columnNames+"\ntypes = "+columnTypes);
				
				if(is1stLine && hasHeaderLine) {
					if(use1stLineAsColNames) {
						// setup statement...
						columnNames = new ArrayList<String>();
						for(int i=0;i<parts.size();i++) {
							columnNames.add(String.valueOf(parts.get(i)));
						}
						//log.debug("colnames: "+columnNames);
					}
				}
				else {
					if(columnTypes==null) {
						columnTypes = new ArrayList<String>();
						for(int i=0;i<parts.size();i++) {
							columnTypes.add(getType(parts.get(i)));
						}
					}
					if(doCreateTable && !tableCreated) {
						log.info("create table: "+getCreateTableSql());
						createTable();
						tableCreated = true;
					}
					if(stmt==null) {
						log.info("insert sql: "+getInsertSql());
						stmt = getStatement();
					}

					boolean rowError = false;
					if(parts.size() < columnTypes.size()) {
						log.warn("row "+counter.input+": #values ["+parts.size()+"] < #columnTypes ["+columnTypes.size()+"]"
							+ (ignoreRowWithWrongNumberOfColumns?" (row ignored)":"")
							);
						rowError = true;
					}

					if(!rowError || !ignoreRowWithWrongNumberOfColumns) {
						for(int i=0;i<columnTypes.size();i++) {
							Object s = parts.get(i);
							/*if(columnTypes.size()<=i) {
								log.warn("coltypes="+columnTypes+" i:"+i);
							}*/
							setStmtMappedValue(stmt, columnTypes.get(i), i, s);
						}
						//log.info("coltypes="+columnTypes+" sql="+getInsertSql()+" parts="+parts);
						int updates = stmt.executeUpdate();
						counter.output += updates;
					}
				}
				
				is1stLine = false;
				}
				catch(RuntimeException e) {
					log.warn("Exception: "+e+" ; parts: "+parts+" ; columnTypes: "+columnTypes);
					if(failonerror) {
						throw e;
					}
				}
				catch(SQLException e) {
					log.warn("SQLException: "+e+" ; parts: "+parts+" ; columnTypes: "+columnTypes);
					if(failonerror) {
						throw e;
					}
				}
			}
			conn.commit();
			log.info( "processedLines: "+counter.input+" ; importedRows: "+counter.output+
				( (counter.successNoInfoCount>0||counter.executeFailedCount>0)?" [successNoInfoCount=="+counter.successNoInfoCount+" ; executeFailedCount=="+counter.executeFailedCount+"]":"")
				);
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		return counter.output;
	}
	
	Object getValue(Cell cell) {
		int type = cell.getCellType();
		Object value = null;
		
		switch (type) {
		case Cell.CELL_TYPE_BOOLEAN:
			value = cell.getBooleanCellValue();
			break;
		case Cell.CELL_TYPE_FORMULA:
			value = cell.getCellFormula();
			break;
		case Cell.CELL_TYPE_NUMERIC:
			value = cell.getNumericCellValue();
			break;
		case Cell.CELL_TYPE_STRING:
			value = cell.getStringCellValue();
			break;
		case Cell.CELL_TYPE_BLANK:
		default:
			break;
		}
		return value;
	}
	
	String getType(Object obj) {
		if(obj==null) { return "string"; } //???
		if(obj instanceof Double) {
			return "double";
		}
		return "string";
	}

}
