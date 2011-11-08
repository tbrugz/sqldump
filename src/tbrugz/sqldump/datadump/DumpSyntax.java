package tbrugz.sqldump.datadump;

import java.io.Writer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import tbrugz.sqldump.Utils;

//TODOne: add syntax: html
//TODOne: add syntax: 'formatted fixed column' (sqlplus-like) 
public abstract class DumpSyntax {
	
	static final Class[] arr = {
		InsertIntoDataDump.class,
		CSVDataDump.class,
		XMLDataDump.class,
		HTMLDataDump.class,
		JSONDataDump.class,
		FFCDataDump.class,
	};
	
	public static String DEFAULT_NULL_VALUE = "";
	
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
		else {
			dateFormatter = Utils.dateFormatter;
		}
		
		String nullValue = prop.getProperty("sqldump.datadump."+getSyntaxId()+".nullvalue");
		if(nullValue!=null) {
			nullValueStr = nullValue;
		}
		String floatLocale = prop.getProperty("sqldump.datadump."+getSyntaxId()+".floatlocale");
		floatFormatter = Utils.getFloatFormatter(floatLocale, getSyntaxId());
	}
	
	public Object getValueNotNull(Object o) {
		if(o==null) { return nullValueStr; }
		return o;
	}
	
	public abstract String getSyntaxId();

	public String getDefaultFileExtension() {
		return getSyntaxId();
	}
	
	public abstract void initDump(String tableName, ResultSetMetaData md) throws Exception;
	
	public abstract void dumpHeader(Writer fos) throws Exception;

	public abstract void dumpRow(ResultSet rs, long count, Writer fos) throws Exception;

	public abstract void dumpFooter(Writer fos) throws Exception;

	public void flushBuffer(Writer fos) throws Exception {}
}
