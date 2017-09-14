package tbrugz.sqldump.datadump;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import tbrugz.sqldump.util.SQLUtils;

/*
 * HSSF: Excel XLS -- application/vnd.ms-excel
 * 
 * https://poi.apache.org/spreadsheet/quick-guide.html
 */
public class PoiXlsSyntax extends OutputStreamDumper {

	//static final Log log = LogFactory.getLog(PoiXlsSyntax.class);
	
	public static final String XLS_SYNTAX_ID = "xls";
	
	public static final String MIME_TYPE = "application/vnd.ms-excel";
	
	Workbook wb;
	Sheet sheet;
	CellStyle cellDateStyle;
	
	@Override
	public void procProperties(Properties prop) {
		//XXX: set date format?
	}

	@Override
	public String getSyntaxId() {
		return XLS_SYNTAX_ID;
	}

	@Override
	public String getMimeType() {
		return MIME_TYPE;
	}
	
	@Override
	public boolean isStateful() {
		return true;
	}

	@Override
	public void dumpHeader(OutputStream os) throws IOException {
		wb = createWorkbook();
		sheet = wb.createSheet(tableName);
		Row row = sheet.createRow(0);
		for(int i=0;i<numCol;i++) {
			String s = lsColNames.get(i);
			Cell cell = row.createCell(i);
			cell.setCellValue(s);
		}
		
		CreationHelper createHelper = wb.getCreationHelper();
		cellDateStyle = wb.createCellStyle();
		cellDateStyle.setDataFormat(createHelper.createDataFormat().getFormat("yyyy-mm-dd"));
	}
	
	@Override
	public void dumpRow(ResultSet rs, long count, OutputStream os)
			throws IOException, SQLException {
		List<Object> vals = SQLUtils.getRowObjectListFromRS(rs, lsColTypes, numCol, false); //XXX: internal resultSet as new sheet??
		Row row = sheet.createRow((int) count+1);
		for(int i=0;i<numCol;i++) {
			Object o = vals.get(i);
			Cell cell = row.createCell(i);
			setCellvalue(cell, o);
		}
	}
	
	Workbook createWorkbook() {
		return new HSSFWorkbook();
	}	
	
	void setCellvalue(Cell cell, Object o) {
		if(o == null) {}
		else if(o instanceof Long) {
			Long ii = (Long)o;
			cell.setCellValue(ii.doubleValue());
		}
		else if(o instanceof Integer) {
			Integer ii = (Integer)o;
			cell.setCellValue(ii.doubleValue());
		}
		else if(o instanceof Double) {
			Double ii = (Double)o;
			cell.setCellValue(ii.doubleValue());
		}
		else if(o instanceof Date) {
			cell.setCellValue((Date) o);
			cell.setCellStyle(cellDateStyle);
		}
		else {
			cell.setCellValue(String.valueOf(o));
		}
	}
	
	@Override
	public void dumpFooter(long count, boolean hasMoreRows, OutputStream os) throws IOException {
		wb.write(os);
		wb.close();
	}

}
