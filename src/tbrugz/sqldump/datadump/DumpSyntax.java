package tbrugz.sqldump.datadump;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.Constraint;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.util.Utils;

public abstract class DumpSyntax implements DumpSyntaxInt {
	
	private static final Log log = LogFactory.getLog(DumpSyntax.class);

	public static final String DEFAULT_NULL_VALUE = "";
	
	public DateFormat dateFormatter;
	//locales: http://www.loc.gov/standards/iso639-2/englangn.html
	public NumberFormat floatFormatter;
	
	public String nullValueStr = DEFAULT_NULL_VALUE;
	
	//public abstract void procProperties(Properties prop);

	public void procStandardProperties(Properties prop) {
		String dateProp = "sqldump.datadump."+getSyntaxId()+".dateformat";
		//log.debug("date prop: '"+dateProp+"'");
		String dateFormat = prop.getProperty(dateProp);
		if(dateFormat!=null) {
			log.debug("["+getSyntaxId()+"] date format: "+dateFormat);
			dateFormatter = new SimpleDateFormat(dateFormat);
		}
		if(dateFormatter==null) {
			String newDefaultDateFormat = prop.getProperty(DataDump.PROP_DATADUMP_DATEFORMAT);
			if(newDefaultDateFormat!=null) {
				log.debug("["+getSyntaxId()+"] (default) date format: "+newDefaultDateFormat);
				dateFormatter = new SimpleDateFormat(newDefaultDateFormat);
			}
			else {
				dateFormatter = DataDumpUtils.dateFormatter;
			}
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
	
	public void initDump(String tableName, List<String> pkCols, ResultSetMetaData md) throws SQLException {
		initDump(null, tableName, pkCols, md);
	}
	
	//XXX: remove from here?
	public Object getValueNotNull(Object o) {
		if(o==null) { return nullValueStr; }
		return o;
	}
	
	//public abstract String getSyntaxId();

	//public abstract String getMimeType();
	
	public String getDefaultFileExtension() {
		return getSyntaxId();
	}
	
	//public abstract void initDump(String tableName, List<String> pkCols, ResultSetMetaData md) throws SQLException;

	public void setImportedFKs(List<FK> fks) {}
	
	public void setAllUKs(List<Constraint> uks) {}
	
	/*
	public abstract void dumpHeader(Writer fos) throws IOException;

	public abstract void dumpRow(ResultSet rs, long count, Writer fos) throws IOException, SQLException;

	public abstract void dumpFooter(long count, Writer fos) throws IOException;
	*/

	public void flushBuffer(Writer fos) throws IOException {}
	
	/**
	 * Should return true if responsable for creating output files
	 * 
	 * BlobDataDump will return true (writes 1 file per table row, partitioning doesn't make sense)
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

	/**
	 * Should return true if imported FKs are used for datadump
	 */
	public boolean usesImportedFKs() {
		return false;
	}

	/**
	 * Should return true if all Unique Keys are used for datadump
	 */
	public boolean usesAllUKs() {
		return false;
	}
	
	@Override
	public Object clone() throws CloneNotSupportedException {
		//throw CloneNotSupportedException?
		//try {
		return super.clone();
		/*} catch (CloneNotSupportedException e) {
			//e.printStackTrace();
			throw new RuntimeException(e);
		}*/
	}
	
	/*public boolean shouldNotWriteBOM() {
		return false;
	}*/
	
	/*public void copyPropsTo(DumpSyntax ds) {
		ds.dateFormatter = this.dateFormatter;
		ds.floatFormatter = this.floatFormatter;
		ds.nullValueStr = this.nullValueStr;
	}*/
	
	//XXX: methods dumpDocHeader, dumpDocFooter -- before dumpHeader/Footer, for dumps with multiple ResultSet
	
	//XXX: method supportResultSetDump()?
	//XXX: method cloneBaseProperties()? duplicateInstance(DumpSyntax)? clone()?
	
	@Override
	public boolean allowWriteBOM() {
		return true;
	}

}
