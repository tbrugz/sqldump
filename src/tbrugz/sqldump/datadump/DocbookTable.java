package tbrugz.sqldump.datadump;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import tbrugz.sqldump.util.SQLUtils;

/*
 * CALS Table spec? https://www.oasis-open.org/specs/tm9502.html
 * 
 * XXX: (option to) dump each element/cell in a line?
 */
public class DocbookTable extends XMLDataDump {
	
	//TODO: set propertuy for ROWSEP_DIVISOR_NUMLINES
	static final int ROWSEP_DIVISOR_NUMLINES = 5; //to ignore, set to, e.g., Integer.MAX_VALUE 
	static final int ROWSEP_DIVISOR_INITGAP = 1;

	@Override
	public void dumpHeader(Writer fos) throws IOException {
		StringBuilder sb = new StringBuilder();
		fos.write(
				//"<?xml version=\"1.0\" ?>\n"+
				"<table id='tab."+this.tableName+"' frame='topbot'>\n"+
				"<title>"+this.tableName+"</title>\n"+
				//"<titleabbrev>"+this.tableName+"</titleabbrev>\n"+
				"<tgroup cols='"+this.numCol+"' align='left' colsep='0' rowsep='0'>\n"
				);
		for(int i=0;i<numCol;i++) {
			sb.append("\t<colspec colname='c"+i+"'/>\n");
		}
		sb.append("<thead>\n\t<row rowsep='1'>");
		for(int i=0;i<numCol;i++) {
			sb.append("<entry>"+lsColNames.get(i)+"</entry>");
		}
		sb.append("</row>\n</thead>\n<tbody>\n");
		fos.write(sb.toString());
	}
	
	@Override
	public void dumpRow(ResultSet rs, long count, Writer fos)
			throws IOException, SQLException {
		List<Object> vals = SQLUtils.getRowObjectListFromRS(rs, lsColTypes, numCol, true);
		
		boolean drawRowSep = ((count + ROWSEP_DIVISOR_INITGAP)%ROWSEP_DIVISOR_NUMLINES) == 0;
		String drawRowSepStr = drawRowSep?" rowsep='1'":"";
		fos.write("\t<row"+drawRowSepStr+">");
		
		for(int i=0;i<numCol;i++) {
			Object value = DataDumpUtils.getFormattedXMLValue(vals.get(i), lsColTypes.get(i), floatFormatter, dateFormatter, nullValueStr);
			fos.write("<entry>"+value+"</entry>");
		}
		fos.write("</row>\n");
	}
	
	@Override
	public void dumpFooter(long count, Writer fos) throws IOException {
		fos.write("</tbody></tgroup>\n</table>\n");
	}
	
	@Override
	public String getSyntaxId() {
		return "dbktable";
	}
	
	@Override
	public String getDefaultFileExtension() {
		//wikipedia: Filename extension: .dbk, .xml
		//return "dbk.xml";
		return "dbk";
	}
	
	@Override
	public String getMimeType() {
		return "application/docbook+xml";
	}
}
