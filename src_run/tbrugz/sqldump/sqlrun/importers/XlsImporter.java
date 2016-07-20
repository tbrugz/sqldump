package tbrugz.sqldump.sqlrun.importers;

import java.io.File;
import java.io.IOException;
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
	
	String importFile;
	String sheetName;
	Integer sheetNumber;
	long linesToSkip = 0;
	boolean hasHeaderLine = true;
	boolean use1stLineAsColNames = false;
	
	static final String[] XLS_AUX_SUFFIXES = {
		SUFFIX_SHEET_NUMBER, SUFFIX_SHEET_NAME
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

		if(!hasHeaderLine && use1stLineAsColNames) {
			log.warn("using '"+SUFFIX_1ST_LINE_AS_COLUMN_NAMES+"' without '"+SUFFIX_1ST_LINE_IS_HEADER+"' is invalid - will be ignored");
		}
	}

	@Override
	public long importData() throws SQLException, InterruptedException, IOException {
		IOCounter counter = new IOCounter();
		try {
			Workbook wb = WorkbookFactory.create(new File(importFile));
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
			
			for (Row row : sheet) {
				counter.input++;
				if(counter.input<=linesToSkip) { continue; }
				
				List<Object> parts = new ArrayList<Object>();
				for (Cell cell : row) {
					parts.add(getValue(cell));
				}

				/*if(is1stLine && !use1stLineAsColNames) {
				}*/
				
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
					if(stmt==null) {
						log.info("sql: "+getInsertSql());
						stmt = getStatement();
					}

					for(int i=0;i<parts.size();i++) {
						Object s = parts.get(i);
						if(columnTypes.size()<=i) {
							log.warn("coltypes="+columnTypes+" i:"+i);
						}
						setStmtValue(stmt, columnTypes.get(i), i, s);
					}
					//log.info("coltypes="+columnTypes+" sql="+getInsertSql()+" parts="+parts);
					int updates = stmt.executeUpdate();
					counter.output += updates;
				}
				is1stLine = false;
			}
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
