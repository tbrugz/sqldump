package tbrugz.sqldump.datadump;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Properties;

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
public class SimpleODS extends OutputStreamDumper {

	static final Log log = LogFactory.getLog(SimpleODS.class);
	
	static final String ODS_SYNTAX_ID = "simple-ods";
	static final String ODS_FILEEXT = "ods";
	static final String PROP_ODS_OUTFILEPATTERN = "sqldump.datadump."+ODS_SYNTAX_ID+".outfilepattern";
	
	/*protected int numCol;
	protected final List<String> lsColNames = new ArrayList<String>();
	protected final List<Class<?>> lsColTypes = new ArrayList<Class<?>>();*/

	//String outFilePattern;
	//String tableName;

	//stateful props
	transient SpreadsheetDocument sd;
	transient Table t;
	
	@Override
	public void procProperties(Properties prop) {
		/*outFilePattern = prop.getProperty(PROP_ODS_OUTFILEPATTERN);
		if(outFilePattern==null) {
			log.warn("prop '"+PROP_ODS_OUTFILEPATTERN+"' must be set");
		}*/
	}

	@Override
	public String getSyntaxId() {
		return ODS_SYNTAX_ID;
	}
	
	@Override
	public String getDefaultFileExtension() {
		return ODS_FILEEXT;
	}

	@Override
	public String getMimeType() {
		return "application/vnd.oasis.opendocument.spreadsheet"; //application/x-vnd.oasis.opendocument.spreadsheet ?
	}

	@Override
	public void initDump(String schema, String tableName, List<String> pkCols,
			ResultSetMetaData md) throws SQLException {
		super.initDump(schema, tableName, pkCols, md);
		/*numCol = md.getColumnCount();
		lsColNames.clear();
		lsColTypes.clear();
		for(int i=0;i<numCol;i++) {
			lsColNames.add(md.getColumnName(i+1));
		}
		for(int i=0;i<numCol;i++) {
			lsColTypes.add(SQLUtils.getClassFromSqlType(md.getColumnType(i+1), md.getPrecision(i+1), md.getScale(i+1)));
		}
		this.tableName = tableName;*/
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
	public void dumpHeader(OutputStream os) throws IOException {
		try {
			sd = newSpreadSheet();
			t = sd.addTable();
			t.setTableName(tableName);

			//dumping header...
			//XXX: add header style - t.getRowByIndex(0).setDefaultCellStyle()?
			for(int i=0;i<numCol;i++) {
				String s = lsColNames.get(i);
				t.getCellByPosition(i, 0).setStringValue(s);
				//t.getColumnByIndex(i).setUseOptimalWidth(true); //set width?
				//log.debug("col: "+lsColNames.get(i)+" / "+t.getColumnByIndex(i).getWidth());
			}
		} catch (Exception e) {
			log.warn("error: "+e);
		}
	}

	@Override
	public void dumpRow(ResultSet rs, long count, OutputStream os)
			throws IOException, SQLException {
		List<Object> vals = SQLUtils.getRowObjectListFromRS(rs, lsColTypes, numCol, false);
		int row = (int)count+1;
		for(int i=0;i<vals.size();i++) {
			Object o = vals.get(i);
			//Class<?> colType = lsColTypes.get(i);
			Cell c = t.getCellByPosition(i, row);
			//XXXxx: set specific value type based on column type
			if(o == null) {}
			else if(o instanceof Long) {
				Long ii = (Long)o;
				//log.info("long "+i+" / "+lsColTypes.get(i)+" / "+ii.doubleValue()+" / "+lsColNames.get(i));
				c.setDoubleValue(ii.doubleValue());
			}
			else if(o instanceof Integer) {
				Integer ii = (Integer)o;
				c.setDoubleValue(ii.doubleValue());
			}
			else if(o instanceof Double) {
				c.setDoubleValue((Double)o);
			}
			else if(o instanceof Date) {
				Calendar cal = new GregorianCalendar();
				cal.setTime((Date)o);
				c.setDateValue(cal);
			}
			else {
				c.setStringValue(String.valueOf(o));
			}
		}
	}

	@Override
	public void dumpFooter(long count, boolean hasMoreRows, OutputStream os) throws IOException {
		try {
			// commons-io...
			//WriterOutputStream out = new WriterOutputStream(fos);
			//XXX: set column width?
			
			/*String filename = outFilePattern
					.replaceAll(DataDump.PATTERN_TABLENAME_FINAL, Matcher.quoteReplacement(tableName) )
					.replaceAll(DataDump.PATTERN_SYNTAXFILEEXT_FINAL, ODS_FILEEXT);
			
			FileOutputStream out = new FileOutputStream(filename);*/
			sd.save(os);
			//out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public boolean isStateful() {
		return true;
	}
	
	@Override
	public boolean acceptsOutputStream() {
		return true;
	}

}
