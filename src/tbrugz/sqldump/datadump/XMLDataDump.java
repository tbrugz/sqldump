package tbrugz.sqldump.datadump;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.resultset.ResultSetArrayAdapter;
import tbrugz.sqldump.util.SQLUtils;
import tbrugz.sqldump.util.Utils;

//XXX: option to define per-column 'row' xml-element? cursors already have "table-name"...
//XXX: option to dump columns as XML atributes. maybe for columns with name like '@<xxx>'?
//XXX: 'alwaysDumpHeaderAndFooter': prop for setting for main dumper & inner (ResultSet) dumpers (using prop per table name?)
//XXX: XMLDataDump to extend AbstractXMLDataDump ? so HTMLDataDump and XMLDataDump would have a common ancestor
public class XMLDataDump extends AbstractDumpSyntax implements DumpSyntaxBuilder, HierarchicalDumpSyntax, Cloneable {

	public enum HeaderFooterDump {
		ALWAYS, IFHASDATA, NEVER;
	}

	static final Log log = LogFactory.getLog(XMLDataDump.class);
	
	static final String XML_SYNTAX_ID = "xml";
	static final String DEFAULT_ROW_ELEMENT = "row";
	
	static final String PROP_ROWELEMENT = "sqldump.datadump.xml.rowelement";
	static final String PROP_DUMPROWELEMENT = "sqldump.datadump.xml.dumprowelement";
	static final String PROP_DUMPNULLVALUES = "sqldump.datadump.xml.dumpnullvalues";
	
	static final String PREFIX_ROWELEMENT4TABLE = "sqldump.datadump.xml.rowelement4table@";
	static final String PREFIX_DUMPROWELEMENT4TABLE = "sqldump.datadump.xml.dumprowelement4table@";
	static final String PROP_DUMPHEADER4INNERTABLES = "sqldump.datadump.xml.dumpheader4innertables";
	static final String PROP_DUMPTABLENAMEASROWTAG = "sqldump.datadump.xml.dumptablenameasrowtag";

	static final String PROP_XML_ESCAPE = "sqldump.datadump.xml.escape";
	static final String PREFIX_ESCAPE4TABLE = "sqldump.datadump.xml.escape4table@";
	static final String PREFIX_ESCAPECOLS_4TABLE = "sqldump.datadump.xml.escapecols4table@";
	static final String PREFIX_NO_ESCAPECOLS_4TABLE = "sqldump.datadump.xml.noescapecols4table@";
	
	//static final String PREFIX_ROWELEMENT4COLUMN = "sqldump.datadump.xml.rowelement4column@";
	//static final String PREFIX_DUMPROWELEMENT4COLUMN = "sqldump.datadump.xml.dumprowelement4column@";

	public XMLDataDump() {
		//this("", HeaderFooterDump.ALWAYS);
		this.padding = "";
		this.dumpHeaderFooter = HeaderFooterDump.ALWAYS;
	}

	/*private XMLDataDump(String padding, HeaderFooterDump dumpHeaderFooter) {
		this.padding = padding;
		this.dumpHeaderFooter = dumpHeaderFooter;
	}*/
	
	String padding;
	HeaderFooterDump dumpHeaderFooter;
	
	//definitions from properties
	protected Properties prop = null;
	String defaultRowElement = DEFAULT_ROW_ELEMENT;
	boolean defaultDumpRowElement = true;
	boolean dumpNullValues = true;
	HeaderFooterDump dumpHeader4InnerTables = HeaderFooterDump.ALWAYS;
	boolean dumpTableNameAsRowTag = false;
	protected boolean escape = true;
	protected boolean escape4table;
	protected boolean useUnderscoreRaw2escape = true;
	List<String> cols2Escape = null;
	List<String> colsNot2Escape = null;

	//dumper properties
	protected String rowElement = defaultRowElement;
	protected boolean dumpRowElement = defaultDumpRowElement;
	
	@Override
	public void procProperties(Properties prop) {
		procStandardProperties(prop);
		defaultRowElement = prop.getProperty(PROP_ROWELEMENT, defaultRowElement);
		defaultDumpRowElement = Utils.getPropBool(prop, PROP_DUMPROWELEMENT, defaultDumpRowElement);
		dumpNullValues = Utils.getPropBool(prop, PROP_DUMPNULLVALUES, dumpNullValues);
		try {
			dumpHeader4InnerTables = HeaderFooterDump.valueOf(prop.getProperty(PROP_DUMPHEADER4INNERTABLES, dumpHeader4InnerTables.name()));
		}
		catch(IllegalArgumentException e) {
			log.warn("invalid argument for '"+PROP_DUMPHEADER4INNERTABLES+"': "+e);
		}
		dumpTableNameAsRowTag = Utils.getPropBool(prop, PROP_DUMPTABLENAMEASROWTAG, dumpTableNameAsRowTag);
		escape = Utils.getPropBool(prop, PROP_XML_ESCAPE, escape);
		
		this.prop = prop;
		postProcProperties();
	}

	@Override
	public void initDump(String schema, String tableName, List<String> pkCols, ResultSetMetaData md) throws SQLException {
		super.initDump(schema, tableName, pkCols, md);

		//XXX: properties that depend on tableName
		rowElement = prop.getProperty(PREFIX_ROWELEMENT4TABLE+tableName, dumpTableNameAsRowTag?tableName:defaultRowElement);
		dumpRowElement = Utils.getPropBool(prop, PREFIX_DUMPROWELEMENT4TABLE+tableName, defaultDumpRowElement);
		escape4table = Utils.getPropBool(prop, PREFIX_ESCAPE4TABLE+tableName, escape);
		cols2Escape = Utils.getStringListFromProp(prop, PREFIX_ESCAPECOLS_4TABLE+tableName, ",");
		colsNot2Escape = Utils.getStringListFromProp(prop, PREFIX_NO_ESCAPECOLS_4TABLE+tableName, ",");
	}
	
	@Override
	public void dumpHeader(Writer fos) throws IOException {
		//XXX: add xml declaration? optional? e.g.: <?xml version="1.0" encoding="UTF-8" ?>
		//see http://www.w3.org/TR/REC-xml/#NT-XMLDecl
		//    http://www.ibm.com/developerworks/xml/library/x-tipdecl/index.html
		if(dumpHeaderFooter.equals(HeaderFooterDump.ALWAYS)) {
			out("<"+tableName+">\n", fos);
		}
	}

	@Override
	public void dumpRow(ResultSet rs, long count, Writer fos) throws IOException, SQLException {
		if(count==0 && dumpHeaderFooter.equals(HeaderFooterDump.IFHASDATA)) {
			out("<"+tableName+">\n", fos);
		}
		if(dumpRowElement) {
			out("\t<"+rowElement+">\n",fos);
		}
		StringBuilder sb = new StringBuilder();
		sb.append("\t\t");
		List<Object> vals = SQLUtils.getRowObjectListFromRS(rs, lsColTypes, numCol, true);
		for(int i=0;i<lsColNames.size();i++) {
			//XXX: prop for selecting ResultSet dumping or not?
			Object origVal = vals.get(i);
			Class<?> ctype = lsColTypes.get(i);
			boolean isResultSet = DataDumpUtils.isResultSet(ctype, origVal);
			boolean isArray = DataDumpUtils.isArray(ctype, origVal);
			if(isResultSet || isArray) {
				ResultSet rsInt = null;
				if(isArray) {
					Object[] objArr = (Object[]) origVal;
					rsInt = new ResultSetArrayAdapter(objArr, false, lsColNames.get(i));
				}
				else {
					rsInt = (ResultSet) origVal;
				}
				
				if(rsInt==null) {
					continue;
				}
				
				dumpAndClearBuffer(sb, fos);
				
				//XXX: one dumper for each column (not each column/row)?
				//XMLDataDump xmldd = new XMLDataDump(this.padding+"\t\t", dumpHeader4InnerTables);
				//xmldd.procProperties(prop);
				XMLDataDump xmldd = innerClone();
				
				/*String rowElement4column = prop.getProperty(PREFIX_ROWELEMENT4COLUMN+lsColNames.get(i));
				if(rowElement4column!=null) {
					Properties propInt = new Properties();
					propInt.putAll(prop);
					propInt.put(PREFIX_ROWELEMENT4TABLE+lsColNames.get(i), rowElement4column);
					xmldd.procProperties(propInt);
				}
				else {
					xmldd.procProperties(prop);
				}*/
				DataDumpUtils.dumpRS(xmldd, rsInt.getMetaData(), rsInt, null, lsColNames.get(i), fos, true);
				//sb.append("\t");
			}
			else {
				String value = DataDumpUtils.getFormattedXMLValue(origVal, ctype, floatFormatter, dateFormatter, doEscape(i));
				if(value==null) {
					if(dumpNullValues) {
						sb.append( "<"+lsColNames.get(i)+">"+ nullValueStr +"</"+lsColNames.get(i)+">" );
					}
				}
				else {
					sb.append( "<"+lsColNames.get(i)+">"+ value +"</"+lsColNames.get(i)+">" );
				}
			}
		}
		dumpAndClearBuffer(sb, fos);
		
		if(dumpRowElement) {
			out("\t</"+rowElement+">\n", fos);
		}
	}
	
	public boolean doEscape(final int i) {
		return escape4table?
				( (colsNot2Escape==null || !colsNot2Escape.contains(lsColNames.get(i))) && (!useUnderscoreRaw2escape || !lsColNames.get(i).endsWith("_RAW") ) ):
				(cols2Escape!=null && cols2Escape.contains(lsColNames.get(i)));
	}
	
	protected void dumpAndClearBuffer(StringBuilder sb, Writer fos) throws IOException {
		String sbuff = sb.toString(); sb.setLength(0);
		if("".equals(sbuff.trim())) {
			return;
		}
		out(sbuff+"\n", fos);
	}

	@Override
	public void dumpFooter(long count, boolean hasMoreRows, Writer fos) throws IOException {
		if(dumpHeaderFooter.equals(HeaderFooterDump.ALWAYS)
				|| (dumpHeaderFooter.equals(HeaderFooterDump.IFHASDATA) && count>0)) {
			out("</"+tableName+">\n", fos);
		}
	}

	protected void out(String s, Writer pw) throws IOException {
		pw.write(padding+s);
	}
	
	@Override
	public String getSyntaxId() {
		return XML_SYNTAX_ID;
	}

	// http://annevankesteren.nl/2004/08/mime-types
	@Override
	public String getMimeType() {
		return "application/xml";
	}
	
	@Override
	public void updateProperties(DumpSyntax ds) {
		if(! (ds instanceof XMLDataDump)) {
			throw new RuntimeException(ds.getClass()+" must be instance of "+this.getClass());
		}
		XMLDataDump dd = (XMLDataDump) ds;
		super.updateProperties(dd);
		
		dd.cols2Escape = this.cols2Escape;
		dd.colsNot2Escape = this.colsNot2Escape;
		dd.defaultDumpRowElement = this.defaultDumpRowElement;
		dd.defaultRowElement = this.defaultRowElement;
		dd.dumpHeader4InnerTables = this.dumpHeader4InnerTables;
		dd.dumpHeaderFooter = this.dumpHeaderFooter;
		dd.dumpNullValues = this.dumpNullValues;
		dd.dumpRowElement = this.dumpRowElement;
		dd.dumpTableNameAsRowTag = this.dumpTableNameAsRowTag;
		dd.escape = this.escape;
		dd.escape4table = this.escape4table;
		dd.padding = this.padding;
		dd.prop = this.prop;
		dd.rowElement = this.rowElement;
		dd.useUnderscoreRaw2escape = this.useUnderscoreRaw2escape;
	}
	
	/*
	@Override
	public XMLDataDump clone() {
		XMLDataDump dd = new XMLDataDump();
		updateProperties(dd);
		return dd;
	}
	*/
	
	@Override
	public XMLDataDump innerClone() {
		try {
			XMLDataDump xmldd = (XMLDataDump) clone();
			xmldd.padding += "\t\t";
			xmldd.dumpHeaderFooter =  dumpHeader4InnerTables;
			return xmldd;
		}
		catch(CloneNotSupportedException e) {
			throw new RuntimeException(e);
		}
	}
	
}
