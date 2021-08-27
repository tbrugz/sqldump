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
	
	String importFile;
	String sheetName;
	Integer sheetNumber;
	long linesToSkip = 0;
	long linesLimit = -1;
	long inputLimit = -1;
	boolean hasHeaderLine = true;
	boolean ignoreRowWithWrongNumberOfColumns = false; //XXX: add prop?
	
	static final String[] XLS_AUX_SUFFIXES = {
		SUFFIX_SHEET_NUMBER, SUFFIX_SHEET_NAME, SUFFIX_1ST_LINE_IS_HEADER, Constants.SUFFIX_1ST_LINE_AS_COLUMN_NAMES, Constants.SUFFIX_DO_CREATE_TABLE
	};
	
	@Override
	public List<String> getAuxSuffixes() {
		return Arrays.asList(XLS_AUX_SUFFIXES);
	}
	
	@Override
	public void setProperties(Properties prop) {
		super.setProperties(prop);
		
		importFile = prop.getProperty(Constants.PREFIX_EXEC+execId+Constants.SUFFIX_IMPORTFILE);
		sheetName = prop.getProperty(Constants.PREFIX_EXEC+execId+SUFFIX_SHEET_NAME);
		Long lSheetNumber = Utils.getPropLong(prop, Constants.PREFIX_EXEC+execId+SUFFIX_SHEET_NUMBER);
		if(lSheetNumber!=null) {
			sheetNumber = lSheetNumber.intValue();
		}
		/*if(sheetName==null && sheetNumber==null) {
			log.info("no sheet number (suffix '"+ SUFFIX_SHEET_NUMBER + "') nor sheet name (suffix '" + SUFFIX_SHEET_NAME + "') defined - will use 1st sheet");
		}*/
		linesToSkip = Utils.getPropLong(prop, Constants.PREFIX_EXEC+execId+Constants.SUFFIX_SKIP_N, linesToSkip);
		hasHeaderLine = Utils.getPropBool(prop, Constants.PREFIX_EXEC+execId+SUFFIX_1ST_LINE_IS_HEADER, hasHeaderLine);
		linesLimit = Utils.getPropLong(prop, Constants.PREFIX_EXEC+execId+Constants.SUFFIX_LIMIT_LINES, linesLimit);
		inputLimit = Utils.getPropLong(prop, Constants.PREFIX_EXEC+execId+Constants.SUFFIX_LIMIT_INPUT, inputLimit);

		if(!hasHeaderLine && use1stLineAsColNames) {
			log.warn("using '"+Constants.SUFFIX_1ST_LINE_AS_COLUMN_NAMES+"' without '"+SUFFIX_1ST_LINE_IS_HEADER+"' is invalid - will be ignored");
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
			long lineOutputCounter = 0;
			PreparedStatement stmt = null;
			boolean tableCreated = false;
			
			// iterating xls': https://poi.apache.org/components/spreadsheet/quick-guide.html#Iterator
			for (Row row : sheet) {
				counter.input++;
				if(counter.input<=linesToSkip) { continue; }
				
				List<Object> parts = new ArrayList<Object>();
				try {
				
				int lastCol = row.getLastCellNum();
				for (int i=0; i<lastCol; i++) {
					Cell cell = row.getCell(i, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL);
					if(cell==null) {
						//log.debug("null cell [#row="+counter.input+";#col="+i+"]");
						parts.add(null);
					}
					else {
						parts.add(getValue(cell));
					}
				}

				//log.info(is1stLine+";"+hasHeaderLine+";"+use1stLineAsColNames+"\nnames = "+columnNames+"\ntypes = "+columnTypes);
				
				if(is1stLine && hasHeaderLine) {
					if(use1stLineAsColNames) {
						// setup statement...
						columnNames = new ArrayList<String>();
						for(int i=0;i<parts.size();i++) {
							columnNames.add(String.valueOf(parts.get(i)));
						}
						finalColumnNames = new ArrayList<String>(columnNames);
						log.info(Constants.SUFFIX_1ST_LINE_AS_COLUMN_NAMES+": colnames: "+columnNames);
					}
				}
				else {
					if(linesLimit >= 0 && lineOutputCounter >= linesLimit) {
						log.info("max (limit) rows reached: "+linesLimit+" [lineOutputCounter="+lineOutputCounter+"]"); 
						break;
					}
					if(inputLimit >= 0 && counter.input >= inputLimit) {
						log.info("max (limit-input) rows reached: "+inputLimit+" [counter.input="+counter.input+"]"); 
						break;
					}
					
					if(columnTypes==null) {
						columnTypes = new ArrayList<String>();
						for(int i=0;i<parts.size();i++) {
							columnTypes.add(getType(parts.get(i)));
						}
						finalColumnTypes = new ArrayList<String>(columnTypes);
					}
					else {
						finalColumnTypes = getFinalColumnTypes(columnTypes);
						finalColumnNames = getFinalColumnNames(columnTypes, columnNames);
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
						rowError = true;
						if(ignoreRowWithWrongNumberOfColumns) {
							log.warn("row "+counter.input+": #values ["+parts.size()+"] < #columnTypes ["+columnTypes.size()+"]"
								+ " (row ignored)"
								);
						}
						else {
							log.debug("row "+counter.input+": #values ["+parts.size()+"] < #columnTypes ["+columnTypes.size()+"]");
							// append nulls to row's parts
							while(parts.size() < columnTypes.size()) {
								parts.add(null);
							}
						}
					}

					if(!rowError || !ignoreRowWithWrongNumberOfColumns) {
						int bindPos = 0;
						for(int i=0;i<columnTypes.size();i++) {
							Object s = parts.get(i);
							/*if(columnTypes.size()<=i) {
								log.warn("coltypes="+columnTypes+" i:"+i);
							}*/
							String colType = columnTypes.get(i);
							if(!skipColumnType(colType)) {
								setStmtMappedValue(stmt, colType, bindPos, s);
								bindPos++;
							}
						}
						//log.info("coltypes="+columnTypes+" sql="+getInsertSql()+" parts="+parts);
						int updates = stmt.executeUpdate();
						counter.output += updates;
						lineOutputCounter++;
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
		if(cell==null) {
			log.warn("null cell??");
			return null;
		}
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
			break;
		default:
			log.warn("Unknown cell type: "+type+" [cell="+cell+"]");
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
