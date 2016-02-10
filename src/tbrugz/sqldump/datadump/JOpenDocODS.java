package tbrugz.sqldump.datadump;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Vector;
import java.util.regex.Matcher;

import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableModel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jopendocument.dom.spreadsheet.SpreadSheet;

import tbrugz.sqldump.util.SQLUtils;

public class JOpenDocODS extends WriterIndependentDumpSyntax {
	static final Log log = LogFactory.getLog(JOpenDocODS.class);
	
	static final String JOPENODS_SYNTAX_ID = "jopen-ods";
	static final String JOPENODS_FILEEXT = "ods";
	static final String PROP_ODS_OUTFILEPATTERN = "sqldump.datadump."+JOPENODS_SYNTAX_ID+".outfilepattern";
	
	protected int numCol;
	protected final List<String> lsColNames = new ArrayList<String>();
	protected final List<Class<?>> lsColTypes = new ArrayList<Class<?>>();

	String outFilePattern;
	String tableName;

	//stateful props
	Vector<Vector<?>> values;
	
	@Override
	public void procProperties(Properties prop) {
		outFilePattern = prop.getProperty(PROP_ODS_OUTFILEPATTERN);
		if(outFilePattern==null) {
			log.warn("prop '"+PROP_ODS_OUTFILEPATTERN+"' must be set");
		}
	}

	@Override
	public String getSyntaxId() {
		return JOPENODS_SYNTAX_ID;
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
	public void initDump(String schema, String tableName, List<String> pkCols,
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
		values = new Vector<Vector<?>>();
	}

	@Override
	public void dumpHeader() throws IOException {
	}

	@Override
	public void dumpRow(ResultSet rs, long count) throws IOException, SQLException {
		List<Object> vals = SQLUtils.getRowObjectListFromRS(rs, lsColTypes, numCol, false);
		values.add(new Vector<Object>(vals));
	}

	@Override
	public void dumpFooter(long count) throws IOException {
		try {
			TableModel model = new DefaultTableModel(values, new Vector<String>(lsColNames));
			
			String filename = outFilePattern
					.replaceAll(DataDump.PATTERN_TABLENAME_FINAL, Matcher.quoteReplacement(tableName) )
					.replaceAll(DataDump.PATTERN_SYNTAXFILEEXT_FINAL, JOPENODS_FILEEXT);
			
			File fout = new File(filename);
			//XXX: add tableName as spreadSheetName?
			SpreadSheet.createEmpty(model).saveAs(fout);
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
		return true; // SpreadSheet.createEmpty().saveAs() accepts only File() - dealing with output ourselves
	}

}
