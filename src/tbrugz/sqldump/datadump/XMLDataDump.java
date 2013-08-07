package tbrugz.sqldump.datadump;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.util.SQLUtils;
import tbrugz.sqldump.util.Utils;

enum HeaderFooterDump {
	ALWAYS, IFHASDATA, NEVER;
}
//XXX: option to define per-column 'row' xml-element? cursors already have "table-name"...
//XXX: option to dump columns as XML atributes. maybe for columns with name like '@<xxx>'?
//XXX: 'alwaysDumpHeaderAndFooter': prop for setting for main dumper & inner (ResultSet) dumpers (using prop per table name?)
public class XMLDataDump extends DumpSyntax {
	
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
	
	//static final String PREFIX_ROWELEMENT4COLUMN = "sqldump.datadump.xml.rowelement4column@";
	//static final String PREFIX_DUMPROWELEMENT4COLUMN = "sqldump.datadump.xml.dumprowelement4column@";

	public XMLDataDump() {
		this("", HeaderFooterDump.ALWAYS);
	}

	public XMLDataDump(String padding, HeaderFooterDump dumpHeaderFooter) {
		this.padding = padding;
		this.dumpHeaderFooter = dumpHeaderFooter;
	}
	
	final String padding;
	final HeaderFooterDump dumpHeaderFooter;
	
	//definitions from properties
	Properties prop = null;
	String defaultRowElement = DEFAULT_ROW_ELEMENT;
	boolean defaultDumpRowElement = true;
	boolean dumpNullValues = true;
	HeaderFooterDump dumpHeader4InnerTables = HeaderFooterDump.ALWAYS;
	boolean dumpTableNameAsRowTag = false;

	//dumper properties
	String tableName;
	int numCol;
	String rowElement = defaultRowElement;
	boolean dumpRowElement = defaultDumpRowElement;
	
	final List<String> lsColNames = new ArrayList<String>();
	final List<Class<?>> lsColTypes = new ArrayList<Class<?>>();
	
	@Override
	public void procProperties(Properties prop) {
		procStandardProperties(prop);
		defaultRowElement = prop.getProperty(PROP_ROWELEMENT, defaultRowElement);
		defaultDumpRowElement = Utils.getPropBool(prop, PROP_DUMPROWELEMENT, defaultDumpRowElement);
		dumpNullValues = Utils.getPropBool(prop, PROP_DUMPNULLVALUES, dumpNullValues);
		dumpHeader4InnerTables = HeaderFooterDump.valueOf(prop.getProperty(PROP_DUMPHEADER4INNERTABLES, dumpHeader4InnerTables.name()));
		dumpTableNameAsRowTag = Utils.getPropBool(prop, PROP_DUMPTABLENAMEASROWTAG, dumpTableNameAsRowTag);
		this.prop = prop;
	}

	@Override
	public void initDump(String tableName, List<String> pkCols, ResultSetMetaData md) throws SQLException {
		this.tableName = tableName;
		numCol = md.getColumnCount();		
		lsColNames.clear();
		lsColTypes.clear();
		for(int i=0;i<numCol;i++) {
			lsColNames.add(md.getColumnName(i+1));
		}
		for(int i=0;i<numCol;i++) {
			lsColTypes.add(SQLUtils.getClassFromSqlType(md.getColumnType(i+1), md.getPrecision(i+1), md.getScale(i+1)));
		}
		
		rowElement = prop.getProperty(PREFIX_ROWELEMENT4TABLE+tableName, dumpTableNameAsRowTag?tableName:defaultRowElement);
		dumpRowElement = Utils.getPropBool(prop, PREFIX_DUMPROWELEMENT4TABLE+tableName, defaultDumpRowElement);
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
			if(ResultSet.class.isAssignableFrom(lsColTypes.get(i))) {
				ResultSet rsInt = (ResultSet) vals.get(i);
				if(rsInt==null) {
					continue;
				}
				
				dumpAndClearBuffer(sb, fos);
				
				//XXX: one dumper for each column (not each column/row)?
				XMLDataDump xmldd = new XMLDataDump(this.padding+"\t\t", dumpHeader4InnerTables);
				xmldd.procProperties(prop);
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
				DataDumpUtils.dumpRS(xmldd, rsInt.getMetaData(), rsInt, lsColNames.get(i), fos, true);
				//sb.append("\t");
			}
			else {
				String value = DataDumpUtils.getFormattedXMLValue(vals.get(i), lsColTypes.get(i), floatFormatter, dateFormatter);
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
	
	void dumpAndClearBuffer(StringBuilder sb, Writer fos) throws IOException {
		String sbuff = sb.toString(); sb.setLength(0);
		if("".equals(sbuff.trim())) {
			return;
		}
		out(sbuff+"\n", fos);
	}

	@Override
	public void dumpFooter(long count, Writer fos) throws IOException {
		if(dumpHeaderFooter.equals(HeaderFooterDump.ALWAYS)
				|| (dumpHeaderFooter.equals(HeaderFooterDump.IFHASDATA) && count>0)) {
			out("</"+tableName+">\n", fos);
		}
	}

	void out(String s, Writer pw) throws IOException {
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
}
