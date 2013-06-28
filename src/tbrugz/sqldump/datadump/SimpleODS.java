package tbrugz.sqldump.datadump;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.odftoolkit.odfdom.dom.element.office.OfficeSpreadsheetElement;
import org.odftoolkit.simple.SpreadsheetDocument;
import org.odftoolkit.simple.table.Cell;
import org.odftoolkit.simple.table.Table;
import org.w3c.dom.Node;

import tbrugz.sqldump.util.SQLUtils;

/*
 * see: http://incubator.apache.org/odftoolkit/simple/document/cookbook/Table.html
 */
public class SimpleODS extends DumpSyntax {

	static final Log log = LogFactory.getLog(SimpleODS.class);
	
	static final String ODS_SYNTAX_ID = "ods";
	static final String PROP_ODS_OUTFILEPATTERN = "sqldump.datadump.ods.outfilepattern";
	
	protected int numCol;
	protected final List<String> lsColNames = new ArrayList<String>();
	protected final List<Class<?>> lsColTypes = new ArrayList<Class<?>>();

	String outFilePattern;
	String tableName;

	//stateful props
	transient SpreadsheetDocument sd;
	transient Table t;
	
	@Override
	public void procProperties(Properties prop) {
		outFilePattern = prop.getProperty(PROP_ODS_OUTFILEPATTERN);
		if(outFilePattern==null) {
			log.warn("prop '"+PROP_ODS_OUTFILEPATTERN+"' must be set");
		}
	}

	@Override
	public String getSyntaxId() {
		return "simple-ods";
	}
	
	@Override
	public String getDefaultFileExtension() {
		return "ods";
	}

	@Override
	public String getMimeType() {
		return "application/vnd.oasis.opendocument.spreadsheet"; //application/x-vnd.oasis.opendocument.spreadsheet ?
	}

	@Override
	public void initDump(String tableName, List<String> pkCols,
			ResultSetMetaData md) throws SQLException {
		numCol = md.getColumnCount();
		lsColNames.clear();
		lsColTypes.clear();
		for(int i=0;i<numCol;i++) {
			lsColNames.add(md.getColumnName(i+1));
		}
		for(int i=0;i<numCol;i++) {
			lsColTypes.add(SQLUtils.getClassFromSqlType(md.getColumnType(i+1), md.getPrecision(i+1), md.getScale(i+1)));
		}
		this.tableName = tableName;
		try {
			sd = newSpreadSheet();
			t = sd.addTable();
			t.setTableName(tableName);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	static SpreadsheetDocument newSpreadSheet() throws Exception {
		SpreadsheetDocument sd = SpreadsheetDocument.newSpreadsheetDocument();
		//int sheetcount = sd.getSheetCount();
		//System.err.println("sheets: "+sheetcount);
		//t = sd.getSheetByIndex(0);
		
		//see: http://mail-archives.apache.org/mod_mbox/incubator-odf-dev/201109.mbox/%3CCAFJd6yRE1ursrbTf=65epx=bCUXyBZHX3DiQcWmYuJ6odX8YZw@mail.gmail.com%3E
		OfficeSpreadsheetElement officeSpreadsheet = sd.getContentRoot();
		Node childNode = officeSpreadsheet.getFirstChild();
		while (childNode != null) {
			officeSpreadsheet.removeChild(childNode);
			childNode = officeSpreadsheet.getFirstChild();
		}
		return sd;
	}

	@Override
	public void dumpHeader(Writer fos) throws IOException {
		//dumping header...
		try {
			//XXX: add header style
			for(int i=0;i<numCol;i++) {
				String s = lsColNames.get(i);
				t.getCellByPosition(i, 0).setStringValue(s);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void dumpRow(ResultSet rs, long count, Writer fos)
			throws IOException, SQLException {
		List<Object> vals = SQLUtils.getRowObjectListFromRS(rs, lsColTypes, numCol, false);
		int row = (int)count+1;
		for(int i=0;i<vals.size();i++) {
			String s = String.valueOf(vals.get(i));
			if(t==null) {
				//log.warn("null table?");
				break;
			}
			Cell c = t.getCellByPosition(i, row);
			if(c==null) {
				log.warn("null! "+i+" / "+row);
			}
			else {
				//XXX: set specific value type based on column type
				c.setStringValue(s);
			}
		}
	}

	@Override
	public void dumpFooter(long count, Writer fos) throws IOException {
		try {
			// commons-io...
			//WriterOutputStream out = new WriterOutputStream(fos);
			String filename = outFilePattern
					.replaceAll(DataDump.PATTERN_TABLENAME_FINAL, Matcher.quoteReplacement(tableName) )
					.replaceAll(DataDump.PATTERN_SYNTAXFILEEXT_FINAL, ODS_SYNTAX_ID);
			
			FileOutputStream out = new FileOutputStream(filename);
			//System.err.println("fout: "+filename);
			/*if(sd==null) {
				log.warn("null document?");
				out.close();
				return;
			}*/
			sd.save(out);
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public boolean isStateful() {
		return true;
	}
	
	@Override
	public boolean isWriterIndependent() {
		return true; // SpreadsheetDocument.save() needs outputstream, not writer - dealing with output ourselves
	}
	
	/*@Override
	public Object clone() throws CloneNotSupportedException {
		return super.clone();
	}*/

}
