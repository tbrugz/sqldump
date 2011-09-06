package tbrugz.sqldump.datadump;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import tbrugz.sqldump.SQLUtils;

//XXX: prop for stylesheet?
public class HTMLDataDump extends DumpSyntax {

	static final String HTML_SYNTAX_ID = "html";
	
	String tableName;
	int numCol;
	List<String> lsColNames = new ArrayList<String>();
	List<Class> lsColTypes = new ArrayList<Class>();
	
	@Override
	public void procProperties(Properties prop) {
	}

	@Override
	public void initDump(String tableName, ResultSetMetaData md) throws SQLException {
		this.tableName = tableName;
		numCol = md.getColumnCount();		
		lsColNames.clear();
		lsColTypes.clear();
		for(int i=0;i<numCol;i++) {
			lsColNames.add(md.getColumnName(i+1));
		}
		for(int i=0;i<numCol;i++) {
			lsColTypes.add(SQLUtils.getClassFromSqlType(md.getColumnType(i+1), md.getScale(i+1)));
		}
	}
	
	@Override
	public void dumpHeader(Writer fos) throws Exception {
		StringBuffer sb = new StringBuffer();
		sb.append("<table id='"+tableName+"'>\n\t<tr>");
		for(int i=0;i<lsColNames.size();i++) {
			sb.append("<th>"+lsColNames.get(i)+"</th>");
		}
		out(sb.toString()+"</tr>\n", fos);
	}

	@Override
	public void dumpRow(ResultSet rs, long count, Writer fos) throws Exception {
		StringBuffer sb = new StringBuffer();
		sb.append("\t"+"<tr>");
		List vals = SQLUtils.getRowObjectListFromRS(rs, lsColTypes, numCol);
		for(int i=0;i<lsColNames.size();i++) {
			sb.append( "<td>"+ vals.get(i) +"</td>");
		}
		sb.append("</tr>");
		out(sb.toString()+"\n", fos);
	}

	@Override
	public void dumpFooter(Writer fos) throws Exception {
		out("</table>", fos);
	}

	void out(String s, Writer pw) throws IOException {
		pw.write(s);
	}
	
	@Override
	public String getSyntaxId() {
		return HTML_SYNTAX_ID;
	}
}
