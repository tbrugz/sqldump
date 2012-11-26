package tbrugz.sqldump.datadump;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import tbrugz.sqldump.util.Utils;

public abstract class DumpSyntax {
	
	static final Class[] arr = {
		InsertIntoDataDump.class,
		CSVDataDump.class,
		XMLDataDump.class,
		HTMLDataDump.class,
		JSONDataDump.class,
		FFCDataDump.class,
		UpdateByPKDataDump.class,
		BlobDataDump.class,
	};
	
	public static final String DEFAULT_NULL_VALUE = "";
	
	public DateFormat dateFormatter;
	//locales: http://www.loc.gov/standards/iso639-2/englangn.html
	public NumberFormat floatFormatter;
	
	public String nullValueStr = DEFAULT_NULL_VALUE;
	
	public static List<Class> getSyntaxes() {
		return Arrays.asList(arr);
	}
	
	public abstract void procProperties(Properties prop);

	public void procStandardProperties(Properties prop) {
		String dateFormat = prop.getProperty("sqldump.datadump."+getSyntaxId()+".dateformat");
		if(dateFormat!=null) {
			dateFormatter = new SimpleDateFormat(dateFormat);
		}
		else if(dateFormatter==null){
			dateFormatter = DataDumpUtils.dateFormatter;
		}
		
		String nullValue = prop.getProperty("sqldump.datadump."+getSyntaxId()+".nullvalue");
		if(nullValue!=null) {
			nullValueStr = nullValue;
		}
		//XXX: test for 'global' properties, like 'sqldump.datadump.floatformat'?
		String floatLocale = prop.getProperty("sqldump.datadump."+getSyntaxId()+".floatlocale");
		String floatFormat = prop.getProperty("sqldump.datadump."+getSyntaxId()+".floatformat");
		floatFormatter = Utils.getFloatFormatter(floatLocale, floatFormat, getSyntaxId());
	}
	
	//XXX: remove from here?
	public Object getValueNotNull(Object o) {
		if(o==null) { return nullValueStr; }
		return o;
	}
	
	public abstract String getSyntaxId();

	public abstract String getMimeType();
	
	public String getDefaultFileExtension() {
		return getSyntaxId();
	}
	
	public abstract void initDump(String tableName, List<String> pkCols, ResultSetMetaData md) throws SQLException;
	
	public abstract void dumpHeader(Writer fos) throws IOException;

	public abstract void dumpRow(ResultSet rs, long count, Writer fos) throws IOException, SQLException;

	public abstract void dumpFooter(Writer fos) throws IOException;

	public void flushBuffer(Writer fos) throws IOException {}
	
	/**
	 * Should return true if responsable for creating output files
	 * 
	 * BlobDataDump will return true (writes 1 file per table row, partitioning doesn't make sense)
	 * @return
	 */
	public boolean isWriterIndependent() {
		return false;
	}

	/**
	 * Should return true if dumpsyntax has buffer
	 * 
	 * FFCDataDump is stateful
	 */
	public boolean isStateful() {
		return false;
	}
	
	//XXX: methods dumpDocHeader, dumpDocFooter -- before dumpHeader/Footer, for dumps with multiple ResultSet
	
	//XXX: method supportResultSetDump()?
	//XXX: method cloneBaseProperties()? duplicateInstance(DumpSyntax)? clone()?

}
