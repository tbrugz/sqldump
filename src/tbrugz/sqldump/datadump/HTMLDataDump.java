package tbrugz.sqldump.datadump;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.util.SQLUtils;
import tbrugz.sqldump.util.Utils;

/*
 * XXX: prop for stylesheet?
 * 
 * see: https://developer.mozilla.org/en/docs/Web/HTML/Element/table
 */
public class HTMLDataDump extends XMLDataDump {

	static final Log log = LogFactory.getLog(HTMLDataDump.class);

	static final String HTML_SYNTAX_ID = "html";
	
	static final String PROP_HTML_PREPEND = "sqldump.datadump.html.prepend";
	static final String PROP_HTML_APPEND = "sqldump.datadump.html.append";
	static final String PROP_HTML_ADD_CAPTION = "sqldump.datadump.html.add-caption";
	static final String PROP_HTML_STYTE_NUMERIC_ALIGN_RIGHT = "sqldump.datadump.html.style.numeric-align-right";
	static final String PROP_HTML_XPEND_INNER_TABLE = "sqldump.datadump.html.xpend-inner-table";
	//static final String PROP_HTML_NULLVALUE_CLASS = "sqldump.datadump.html.nullvalue-class";
	//XXX add props 'sqldump.datadump.html.inner-table.[prepend|append]' ??
	
	//protected String tableName;
	//protected int numCol;
	//protected List<String> lsColNames = new ArrayList<String>();
	//protected List<Class<?>> lsColTypes = new ArrayList<Class<?>>();
	
	protected static final String DEFAULT_PADDING = "";
	protected static final boolean DEFAULT_ADD_CAPTION = false;

	protected final String padding;
	protected final boolean innerTable;
	
	protected String prepend = null;
	protected String append = null;
	//protected String nullValueClass = null;
	protected boolean dumpCaptionElement = DEFAULT_ADD_CAPTION;
	//TODO: prop for 'dumpColElement'
	protected boolean dumpColElement = false;
	protected boolean dumpStyleNumericAlignRight = false;
	protected boolean xpendInnerTable = true;
	
	public HTMLDataDump() {
		this(DEFAULT_PADDING, false);
	}
	
	public HTMLDataDump(String padding, boolean innerTable) {
		this.padding = padding;
		this.innerTable = innerTable;
	}
	
	@Override
	public void procProperties(Properties prop) {
		super.procProperties(prop);
		//procStandardProperties(prop);
		prepend = prop.getProperty(PROP_HTML_PREPEND);
		append = prop.getProperty(PROP_HTML_APPEND);
		//nullValueClass = prop.getProperty(PROP_HTML_NULLVALUE_CLASS);
		dumpCaptionElement = Utils.getPropBool(prop, PROP_HTML_ADD_CAPTION, DEFAULT_ADD_CAPTION);
		dumpStyleNumericAlignRight = Utils.getPropBool(prop, PROP_HTML_STYTE_NUMERIC_ALIGN_RIGHT, dumpStyleNumericAlignRight);
		xpendInnerTable = Utils.getPropBool(prop, PROP_HTML_XPEND_INNER_TABLE, xpendInnerTable);
	}

	/*@Override
	public void initDump(String tableName, List<String> pkCols, ResultSetMetaData md) throws SQLException {
		this.tableName = tableName;
		numCol = md.getColumnCount();
		lsColNames.clear();
		lsColTypes.clear();
		for(int i=0;i<numCol;i++) {
			lsColNames.add(md.getColumnName(i+1));
			lsColTypes.add(SQLUtils.getClassFromSqlType(md.getColumnType(i+1), md.getPrecision(i+1), md.getScale(i+1)));
		}
	}*/
	
	@Override
	public void dumpHeader(Writer fos) throws IOException {
		tablePrepend(fos);
		//if(prepend!=null && (!innerTable || xpendInnerTable)) { out(prepend, fos); }
		StringBuilder sb = new StringBuilder();
		sb.append("<table class='"+tableName+"'>");
		if(dumpStyleNumericAlignRight) {
			appendStyleNumericAlignRight(sb);
		}
		if(dumpCaptionElement){
			//XXX: set caption?
			sb.append("\n\t<caption>" + (schemaName!=null?schemaName+".":"") + tableName + "</caption>");
		}
		if(dumpColElement) {
			sb.append("\n<colgroup>");
			for(int i=0;i<lsColNames.size();i++) {
				sb.append("\n\t<col colname=\""+lsColNames.get(i)+"\" type=\""+lsColTypes.get(i).getSimpleName()+"\"/>");
			}
			sb.append("\n</colgroup>");
		}
		//XXX: add thead?
		sb.append("\n\t<tr>");
		for(int i=0;i<lsColNames.size();i++) {
			sb.append("<th>"+lsColNames.get(i)+"</th>");
		}
		out(sb.toString()+"</tr>\n", fos);
		//XXX: add tbody?
	}
	
	protected void appendStyleNumericAlignRight(StringBuilder sb) {
		List<String> styleSelector = new ArrayList<String>();
		for(int i=0;i<lsColNames.size();i++) {
			if(lsColTypes.get(i).equals(Integer.class) || lsColTypes.get(i).equals(Double.class)) {
				styleSelector.add("table."+tableName+" td:nth-child("+(i+1)+")");
			}
		}
		if(styleSelector.size()>0) {
			sb.append("\n\t<style>\n\t\t").append(Utils.join(styleSelector, ", ")).append(" { text-align: right; }\n\t</style>");
		}
	}

	@Override
	public void dumpRow(ResultSet rs, long count, Writer fos) throws IOException, SQLException {
		dumpRow(rs, count, null, fos);
	}
	
	public void dumpRow(ResultSet rs, long count, String clazz, Writer fos) throws IOException, SQLException {
		StringBuilder sb = new StringBuilder();
		sb.append("\t"+"<tr"+(clazz!=null?" class=\""+clazz+"\"":"")+">");
		List<Object> vals = SQLUtils.getRowObjectListFromRS(rs, lsColTypes, numCol, true);
		for(int i=0;i<lsColNames.size();i++) {
			if(ResultSet.class.isAssignableFrom(lsColTypes.get(i))) {
				ResultSet rsInt = (ResultSet) vals.get(i);
				
				if(rsInt==null) {
					//log.warn("ResultSet is null");
					sb.append("<td></td>");
					continue;
				}
				
				out(sb.toString()+"<td>\n", fos);
				sb = new StringBuilder();
				
				HTMLDataDump htmldd = new HTMLDataDump(this.padding+"\t\t", true);
				//htmldd.padding = this.padding+"\t\t";
				//log.info(":: "+rsInt+" / "+lsColNames);
				htmldd.procProperties(prop);
				DataDumpUtils.dumpRS(htmldd, rsInt.getMetaData(), rsInt, lsColNames.get(i), fos, true);
				sb.append("\n\t</td>");
			}
			else {
				Object origVal = vals.get(i);
				String value = DataDumpUtils.getFormattedXMLValue(origVal, lsColTypes.get(i), floatFormatter, dateFormatter, nullValueStr,
						doEscape(i));
				//Object value = getValueNotNull( vals.get(i) );
				//XXX add type attribute?
				sb.append( "<td" + (origVal==null?" null=\"true\"":"") + ">"+ value +"</td>");
			}
		}
		sb.append("</tr>");
		out(sb.toString()+"\n", fos);
	}

	@Override
	public void dumpFooter(long count, Writer fos) throws IOException {
		//XXX: add /tbody?
		out("</table>", fos);
		//if(append!=null && (!innerTable || xpendInnerTable)) { out(append, fos); }
		tableAppend(fos);
	}

	protected void tablePrepend(Writer fos) throws IOException {
		if(prepend!=null && (!innerTable || xpendInnerTable)) { out(prepend, fos); }
	}
	
	protected void tableAppend(Writer fos) throws IOException {
		if(append!=null && (!innerTable || xpendInnerTable)) { out(append, fos); }
	}
	
	/*protected void out(String s, Writer pw) throws IOException {
		pw.write(padding+s);
	}*/
	
	@Override
	public String getSyntaxId() {
		return HTML_SYNTAX_ID;
	}

	@Override
	public String getMimeType() {
		return "text/html";
	}
}
