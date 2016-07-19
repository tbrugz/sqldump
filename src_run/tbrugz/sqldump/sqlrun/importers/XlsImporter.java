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
	
	String importFile;
	String sheetName;
	Integer sheetNumber;
	int linesToSkip = 1;
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
		if(sheetName==null && sheetNumber==null) {
			log.info("no sheet number (suffix '"+ SUFFIX_SHEET_NUMBER + "') nor sheet name (suffix '" + SUFFIX_SHEET_NAME + "') defined - will use 1st sheet");
		}
	}

	@Override
	public long importData() throws SQLException, InterruptedException, IOException {
		IOCounter counter = new IOCounter();
		try {
			Workbook wb = WorkbookFactory.create(new File(importFile));
			Sheet sheet = null;
			if(sheetNumber!=null) {
				sheet = wb.getSheetAt(sheetNumber);
			}
			else if(sheetName!=null) {
				sheet = wb.getSheet(sheetName);
			}
			else {
				sheet = wb.getSheetAt(0);
			}
			
			boolean is1stLine = true;
			PreparedStatement stmt = null;
			
			for (Row row : sheet) {
				counter.input++;
				if(counter.input<=linesToSkip) { continue; }
				
				List<String> parts = new ArrayList<String>();
				for (Cell cell : row) {
					// Do something here
					int type = cell.getCellType();
					String value = null;
					switch (type) {
					case Cell.CELL_TYPE_BOOLEAN:
						value = String.valueOf(cell.getBooleanCellValue());
						break;
					case Cell.CELL_TYPE_FORMULA:
						value = String.valueOf(cell.getCellFormula());
						break;
					case Cell.CELL_TYPE_NUMERIC:
						value = String.valueOf(cell.getNumericCellValue());
						break;
					case Cell.CELL_TYPE_STRING:
						value = cell.getStringCellValue();
						break;
					case Cell.CELL_TYPE_BLANK:
					default:
						break;
					}
					parts.add(value);
				}

				if(is1stLine) {
					if(columnTypes==null) {
						columnTypes = new ArrayList<String>();
						for(int i=0;i<parts.size();i++) {
							columnTypes.add("string");
						}
					}
					stmt = getStatement();
				}
				if(is1stLine && use1stLineAsColNames) {
					// setup statement...
				}
				else {
					for(int i=0;i<parts.size();i++) {
						String s = parts.get(i);
						if(columnTypes.size()<=i) {
							log.warn("coltypes="+columnTypes+" i:"+i);
						}
						setStmtValue(stmt, columnTypes.get(i), i, s);
					}
					log.warn("coltypes="+columnTypes+" sql="+getInsertSql()+" parts="+parts);
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

}
