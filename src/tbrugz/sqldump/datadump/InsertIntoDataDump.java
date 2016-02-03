package tbrugz.sqldump.datadump;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.def.Defs;
import tbrugz.sqldump.util.SQLUtils;
import tbrugz.sqldump.util.StringDecorator;
import tbrugz.sqldump.util.Utils;

public class InsertIntoDataDump extends DumpSyntax {

	private static final Log log = LogFactory.getLog(InsertIntoDataDump.class);
	
	static final String INSERTINTO_SYNTAX_ID = "insertinto";
	static final String INSERTINTO_PREFIX = "sqldump.datadump.insertinto";
	
	static final String PROP_DATADUMP_INSERTINTO_WITHCOLNAMES = "sqldump.datadump.insertinto.withcolumnnames";
	static final String PROP_INSERTINTO_DUMPCURSORS = "sqldump.datadump.insertinto.dumpcursors";
	//XXX: option/prop to include or not columns that are cursor expressions (ResultSets) as null
	static final String PROP_INSERTINTO_HEADER = "sqldump.datadump.insertinto.header";
	static final String PROP_INSERTINTO_FOOTER = "sqldump.datadump.insertinto.footer";
	//compactmode/multiple rows: compatible with mysql, sqlserver, postgresql, ...? oracle: 'insert all ...'
	//XXX: compactmode: maximum number of rows in one insert statement?
	static final String PROP_INSERTINTO_COMPACT = "sqldump.datadump.insertinto.compactmode";
	static final String PROP_INSERTINTO_QUOTESQL = INSERTINTO_PREFIX+".quotesql";
	static final String PROP_INSERTINTO_DUMPSCHEMA = INSERTINTO_PREFIX+".dumpschema";
	
	static final String TABLENAME_PATTERN = Pattern.quote(Defs.addSquareBraquets(Defs.PATTERN_TABLENAME));
	
	static final String COMPACTMODE_IDENT = "  ";
	static final DateFormat sqlDefaultDateFormatter = new SimpleDateFormat("''yyyy-MM-dd''"); // "DATE ''yyyy-MM-dd''" ?

	protected String tableName;
	protected String schemaName;
	protected String fullTableName4Dump;
	protected int numCol;
	String colNames;
	protected final List<String> lsColNames = new ArrayList<String>();
	protected final List<Class<?>> lsColTypes = new ArrayList<Class<?>>();
	protected List<String> pkCols = null;
	
	boolean doColumnNamesDump = true;
	boolean doDumpCursors = false;
	boolean dumpCompactMode = false;
	boolean doQuoteAllSqlIds = false;
	boolean doDumpSchemaName = false;
	
	String header;
	String footer;
	
	Properties prop;
	
	@Override
	public void procProperties(Properties prop) {
		String dbmsDatePattern = DBMSResources.instance().databaseSpecificFeaturesClass().sqlDefaultDateFormatPattern();
		if(dbmsDatePattern!=null) {
			log.debug("dbms default date format: "+dbmsDatePattern);
			dateFormatter = new SimpleDateFormat(dbmsDatePattern);
		}
		else {
			dateFormatter = sqlDefaultDateFormatter;
		}
		procStandardProperties(prop);
		doColumnNamesDump = Utils.getPropBool(prop, PROP_DATADUMP_INSERTINTO_WITHCOLNAMES, doColumnNamesDump);
		doDumpCursors = Utils.getPropBool(prop, PROP_INSERTINTO_DUMPCURSORS, doDumpCursors);
		dumpCompactMode = Utils.getPropBool(prop, PROP_INSERTINTO_COMPACT, dumpCompactMode);
		doQuoteAllSqlIds = Utils.getPropBool(prop, PROP_INSERTINTO_QUOTESQL, doQuoteAllSqlIds);
		doDumpSchemaName = Utils.getPropBool(prop, PROP_INSERTINTO_DUMPSCHEMA, doDumpSchemaName);
		//XXX replace [schemaname] in header & footer ?
		header = prop.getProperty(PROP_INSERTINTO_HEADER);
		footer = prop.getProperty(PROP_INSERTINTO_FOOTER);
		this.prop = prop;
	}
	
	@Override
	public String getDefaultFileExtension() {
		return "sql";
	}

	@Override
	public void initDump(String schemaName, String tableName, List<String> pkCols, ResultSetMetaData md) throws SQLException {
		this.tableName = tableName;
		this.schemaName = schemaName;
		String tableName4Dump = tableName;
		String schemaName4Dump = schemaName;
		this.pkCols = pkCols;
		numCol = md.getColumnCount();
		lsColTypes.clear();
		lsColNames.clear();
		//List<String> lsColNames = new ArrayList<String>();
		for(int i=0;i<numCol;i++) {
			lsColNames.add(md.getColumnName(i+1));
		}
		for(int i=0;i<numCol;i++) {
			lsColTypes.add(SQLUtils.getClassFromSqlType(md.getColumnType(i+1), md.getPrecision(i+1), md.getScale(i+1)));
		}
		if(doQuoteAllSqlIds) { //quote all
			String quote = DBMSResources.instance().getIdentifierQuoteString();
			StringDecorator quoteAllDecorator = new StringDecorator.StringQuoterDecorator(quote);
			colNames = "("+Utils.join(lsColNames, ", ", quoteAllDecorator)+")";
			tableName4Dump = quoteAllDecorator.get(tableName);
			schemaName4Dump = quoteAllDecorator.get(schemaName);
		}
		else {
			colNames = "("+Utils.join(lsColNames, ", ")+")";
		}
		fullTableName4Dump = getTable4Dump(schemaName4Dump, tableName4Dump);
	}

	//XXX: option to dump ResultSet columns
	@Override
	public void dumpRow(ResultSet rs, long count, Writer fos) throws IOException, SQLException {
		List<Object> vals = SQLUtils.getRowObjectListFromRS(rs, lsColTypes, numCol, doDumpCursors);
		String valsStr = DataDumpUtils.join4sql(vals, dateFormatter, ", ");
		if(dumpCompactMode) {
			out((count>0?COMPACTMODE_IDENT+",":COMPACTMODE_IDENT+" ")+
				"("+
				valsStr+
				")", fos);
		}
		else {
			out("insert into "+fullTableName4Dump+
				(doColumnNamesDump?" "+colNames:"")+
				" values ("+
				valsStr+
				");", fos);
		}
		
		if(doDumpCursors) {
			for(int i=0;i<lsColNames.size();i++) {
				if(ResultSet.class.isAssignableFrom(lsColTypes.get(i))) {
					ResultSet rsInt = (ResultSet) vals.get(i);
					if(rsInt==null) { continue; }
					InsertIntoDataDump iidd = new InsertIntoDataDump();
					iidd.procProperties(prop);
					DataDumpUtils.dumpRS(iidd, rsInt.getMetaData(), rsInt, null, lsColNames.get(i), fos, true);
				}
			}
		}
	}
	
	protected void out(String s, Writer pw) throws IOException {
		pw.write(s+"\n");
	}

	@Override
	public void dumpHeader(Writer fos) throws IOException {
		if(header!=null) {
			String thisHeader = header.replaceAll(TABLENAME_PATTERN, Matcher.quoteReplacement(tableName));
			out(thisHeader, fos);
		}
		if(dumpCompactMode) {
			out("insert into "+fullTableName4Dump+
				(doColumnNamesDump?" "+colNames:"")+
				" values", fos);
		}
	}

	@Override
	public void dumpFooter(long count, Writer fos) throws IOException {
		if(dumpCompactMode) {
			out(COMPACTMODE_IDENT+";", fos);
		}
		if(footer!=null) {
			String thisFooter = footer.replaceAll(TABLENAME_PATTERN, Matcher.quoteReplacement(tableName));
			out(thisFooter, fos);
		}
		//always add (empty) footer?
		/*
		else {
			out("", fos);
		}
		*/
	}
	
	protected String getTable4Dump(String schema, String table) {
		return (doDumpSchemaName&&schema!=null?schema+".":"")+table;
	}

	@Override
	public String getSyntaxId() {
		return INSERTINTO_SYNTAX_ID;
	}

	/*
	 * see:
	 * http://en.wikipedia.org/wiki/SQL
	 * http://www.iana.org/assignments/media-types/application/sql
	 */
	@Override
	public String getMimeType() {
		return "application/sql";
	}
}
