package tbrugz.sqldump.datadump;

import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

/*
 * XSSF: Excel XLSX -- application/vnd.openxmlformats-officedocument.spreadsheetml.sheet
 */
public class PoiXlsxSyntax extends PoiXlsSyntax {

	public static final String XLS_SYNTAX_ID = "xlsx";
	
	public static final String MIME_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
	
	@Override
	public String getSyntaxId() {
		return XLS_SYNTAX_ID;
	}

	@Override
	public String getMimeType() {
		return MIME_TYPE;
	}
	
	@Override
	Workbook createWorkbook() {
		return new XSSFWorkbook();
	}
	
}
