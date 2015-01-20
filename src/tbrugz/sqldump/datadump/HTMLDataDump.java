package tbrugz.sqldump.datadump;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.util.SQLUtils;

//XXX: prop for stylesheet?
//XXXdone: should extend XMLDataDump?
public class HTMLDataDump extends XMLDataDump {

	static final Log log = LogFactory.getLog(HTMLDataDump.class);

	static final String HTML_SYNTAX_ID = "html";
	
	static final String PROP_HTML_PREPEND = "sqldump.datadump.html.prepend";
	static final String PROP_HTML_APPEND = "sqldump.datadump.html.append";
	
	//protected String tableName;
	//protected int numCol;
	//protected List<String> lsColNames = new ArrayList<String>();
	//protected List<Class<?>> lsColTypes = new ArrayList<Class<?>>();

	protected final String padding;
	
	protected String prepend = null;
	protected String append = null;
	//TODO: prop for 'dumpColElement'
	protected final boolean dumpColElement = false;
	
	public HTMLDataDump() {
		this("");
	}
	
	public HTMLDataDump(String padding) {
		this.padding = padding;
	}
	
	@Override
	public void procProperties(Properties prop) {
		super.procProperties(prop);
		//procStandardProperties(prop);
		prepend = prop.getProperty(PROP_HTML_PREPEND);
		append = prop.getProperty(PROP_HTML_APPEND);
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
		if(prepend!=null) { out(prepend, fos); }
		out("<table class='"+tableName+"'>", fos);
		StringBuffer sb = new StringBuffer();
		if(dumpColElement) {
			for(int i=0;i<lsColNames.size();i++) {
				sb.append("\n\t<col class=\"type_"+lsColTypes.get(i).getSimpleName()+"\"/>");
			}
		}
		sb.append("\n\t<tr>");
		for(int i=0;i<lsColNames.size();i++) {
			sb.append("<th>"+lsColNames.get(i)+"</th>");
		}
		out(sb.toString()+"</tr>\n", fos);
	}

	@Override
	public void dumpRow(ResultSet rs, long count, Writer fos) throws IOException, SQLException {
		dumpRow(rs, count, null, fos);
	}
	
	public void dumpRow(ResultSet rs, long count, String clazz, Writer fos) throws IOException, SQLException {
		StringBuffer sb = new StringBuffer();
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
				sb = new StringBuffer();
				
				HTMLDataDump htmldd = new HTMLDataDump(this.padding+"\t\t");
				//htmldd.padding = this.padding+"\t\t";
				//log.info(":: "+rsInt+" / "+lsColNames);
				htmldd.procProperties(prop);
				DataDumpUtils.dumpRS(htmldd, rsInt.getMetaData(), rsInt, lsColNames.get(i), fos, true);
				sb.append("\n\t</td>");
			}
			else {
				Object value = DataDumpUtils.getFormattedXMLValue(vals.get(i), lsColTypes.get(i), floatFormatter, dateFormatter, nullValueStr,
						doEscape(i));
				//Object value = getValueNotNull( vals.get(i) );
				sb.append( "<td>"+ value +"</td>");
			}
		}
		sb.append("</tr>");
		out(sb.toString()+"\n", fos);
	}

	@Override
	public void dumpFooter(long count, Writer fos) throws IOException {
		out("</table>", fos);
		if(append!=null) { out(append, fos); }
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
