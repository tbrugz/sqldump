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
import tbrugz.sqldump.Utils;

/*
 * XXX: option to use 'milliseconds in Universal Coordinated Time (UTC) since epoch' as date
 *      dateformat? maybe with JSR-310 (http://threeten.sourceforge.net/)?
 * 
 * see: http://weblogs.asp.net/bleroy/archive/2008/01/18/dates-and-json.aspx
 *      http://msdn.microsoft.com/en-us/library/bb299886.aspx 
 *      
 * TODO: option to output as hash (using pkcols)
 */
public class JSONDataDump extends DumpSyntax {

	static final String JSON_SYNTAX_ID = "json";
	
	String tableName;
	int numCol;
	List<String> lsColNames = new ArrayList<String>();
	List<Class> lsColTypes = new ArrayList<Class>();
	
	boolean usePK = false; //XXX: option to set prop usePK
	List<String> pkCols;
	
	@Override
	public void procProperties(Properties prop) {
		procStandardProperties(prop);
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
			lsColTypes.add(SQLUtils.getClassFromSqlType(md.getColumnType(i+1), md.getScale(i+1)));
		}
		if(usePK) {
			this.pkCols = pkCols;
		}
		//if(pkCols==null) { usePK = false; } else { usePK = true; }
	}
	
	@Override
	public void dumpHeader(Writer fos) throws Exception {
		out("{ \""+tableName+"\": "
			+(this.pkCols!=null?"{":"[")
			+"\n", fos);
	}

	@Override
	public void dumpRow(ResultSet rs, long count, Writer fos) throws Exception {
		StringBuffer sb = new StringBuffer();
		sb.append("\t"+(count==0?"":","));
		if(this.pkCols!=null) {
			sb.append("\"");
			for(int i=0;i<pkCols.size();i++) {
				if(i>0) { sb.append("_"); }
				sb.append(rs.getString(pkCols.get(i)));
			}
			sb.append("\": ");
		}
		sb.append("{");
		
		List vals = SQLUtils.getRowObjectListFromRS(rs, lsColTypes, numCol);
		for(int i=0;i<lsColNames.size();i++) {
			try {
				sb.append((i==0?"":", ") + "\"" + lsColNames.get(i) + "\"" + ": " + Utils.getFormattedJSONValue( vals.get(i), dateFormatter ));
			}
			catch(Exception e) {
				System.err.println(lsColNames+" / "+vals);
				//e.printStackTrace();
			}
		}
		sb.append("}");
		out(sb.toString()+"\n", fos);
	}

	@Override
	public void dumpFooter(Writer fos) throws Exception {
		out("  "+(usePK?"}":"]")+"\n}",fos);
	}

	void out(String s, Writer pw) throws IOException {
		pw.write(s);
	}
	
	@Override
	public String getSyntaxId() {
		return JSON_SYNTAX_ID;
	}
}
