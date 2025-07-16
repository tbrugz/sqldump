package tbrugz.sqldump.datadump;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import tbrugz.sqldump.util.Utils;

/*
 * XSSF: Excel XLSX -- application/vnd.openxmlformats-officedocument.spreadsheetml.sheet
 */
public class PoiXlsxSyntax extends PoiXlsSyntax {

	public static final String XLSX_SYNTAX_ID = "xlsx";
	
	public static final String MIME_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
	
	public static final String SUFFIX_USE_STREAMING = "use-streaming";

	boolean useStreaming = false;
	int rowAccessWindowSize = 100;
	
	@Override
	public void procProperties(Properties prop) {
		super.procProperties(prop);
		useStreaming = Utils.getPropBool(prop, fullPrefix() + SUFFIX_USE_STREAMING, useStreaming);
	}

	@Override
	public String getSyntaxId() {
		return XLSX_SYNTAX_ID;
	}

	@Override
	public String getMimeType() {
		return MIME_TYPE;
	}
	
	@Override
	Workbook createWorkbook() {
		if(!useStreaming) {
			return new XSSFWorkbook();
		}
		else {
			return new SXSSFWorkbook(rowAccessWindowSize);
		}
	}
	
	@Override
	public void autoSizeColumns() {
		if(sheet instanceof SXSSFSheet) {
			SXSSFSheet ssheet = (SXSSFSheet) sheet;
			for(int i=0;i<numCol;i++) {
				//sheet.autoSizeColumn(i);
				ssheet.trackColumnForAutoSizing(i);
			}
		}
		super.autoSizeColumns();
	}

	@Override
	public void dumpFooter(long count, boolean hasMoreRows, OutputStream os) throws IOException {
		super.dumpFooter(count, hasMoreRows, os);
		/*
		wb.write(os);
		wb.close();
		*/
		if(wb instanceof SXSSFWorkbook) {
			((SXSSFWorkbook)wb).dispose();
		}
	}

}
