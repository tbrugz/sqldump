package tbrugz.sqldiff.datadiff;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.datadump.DataDumpUtils;
import tbrugz.sqldump.datadump.HTMLDataDump;
import tbrugz.sqldump.util.SQLUtils;

public class HTMLDiff extends HTMLDataDump implements DiffSyntax {

	static final Log log = LogFactory.getLog(HTMLDiff.class);
	
	boolean shouldFlush = false;
	boolean cellDiffAddFirst = false;
	
	static final String PROP_HTML_DIFF_PREPEND = "sqldump.datadump.html.diff.prepend";
	static final String PROP_HTML_DIFF_APPEND = "sqldump.datadump.html.diff.append";
	
	public HTMLDiff() {
		this.nullValueStr = UNICODE_NULL;
	}
	
	@Override
	public void procProperties(Properties prop) {
		super.procProperties(prop);
		prepend = prop.getProperty(PROP_HTML_DIFF_PREPEND, prepend);
		append = prop.getProperty(PROP_HTML_DIFF_APPEND, append);
	}
	
	@Override
	public boolean dumpUpdateRowIfNotEquals(ResultSet rsSource,
			ResultSet rsTarget, long count, boolean alsoDumpIfEquals, Writer w) throws IOException,
			SQLException {
		List<Object> valsS = SQLUtils.getRowObjectListFromRS(rsSource, lsColTypes, numCol, true);
		List<Object> valsT = SQLUtils.getRowObjectListFromRS(rsTarget, lsColTypes, numCol, true);
		
		return dumpUpdateRowIfNotEquals(valsS, valsT, count, alsoDumpIfEquals, rsTarget, w);
	}

	@Override
	public boolean dumpUpdateRowIfNotEquals(List<Object> valsS, List<Object> valsT,
			long count, boolean dumpIfEquals, ResultSet dumpIfEqualsRS, Writer w) throws
			IOException, SQLException {
		List<Object> fvalS = getFormattedVals(valsS);
		List<Object> fvalT = getFormattedVals(valsT);
		if(!equals(fvalS, fvalT)) {
			dumpRowValues(fvalS, fvalT, count, "change", w);
			if(shouldFlush) { flush(w); }
			return true;
		}
		else {
			if(dumpIfEquals) {
				dumpRow(dumpIfEqualsRS, count, "equal", w);
			}
			if(shouldFlush) { flush(w); }
			return false;
		}
	}

	@Override
	public void dumpUpdateRow(ResultSet rsSource, ResultSet rsTarget,
			long count, Writer w) throws IOException, SQLException {
		log.warn("dumpUpdateRow: not implemented");
	}
	
	@Override
	public void dumpRow(ResultSet rs, long count, Writer w) throws IOException, SQLException {
		dumpRow(rs, count, "add", w);
		if(shouldFlush) { flush(w); }
	}

	@Override
	public void dumpDeleteRow(ResultSet rs, long count, Writer w) throws IOException, SQLException {
		dumpRow(rs, count, "remove", w);
		//log.info("dumpDelete: count="+count);
		if(shouldFlush) { flush(w); }
	}
	
	@Override
	public void dumpStats(long insertCount, long updateCount, long deleteCount, long identicalRowsCount,
			long sourceRowCount, long targetRowCount, Writer w) throws IOException, SQLException {
	}
	
	public void dumpRowValues(List<Object> valsS, List<Object> valsT, long count, String clazz, Writer fos) throws IOException, SQLException {
		StringBuilder sb = new StringBuilder();
		sb.append("\t"+"<tr"+(clazz!=null?" class=\""+clazz+"\"":"")+">");
		for(int i=0;i<lsColNames.size();i++) {
			Object valueS = valsS.get(i);
			Object valueT = valsT.get(i);
			if(valueS.equals(valueT)) {
				sb.append( "<td>"+ valueS +"</td>" );
			}
			else {
				sb.append("<td>");
				if(cellDiffAddFirst) {
					sb.append( "<span class=\"add\">"+ valueT +"</span><br><span class=\"remove\">"+ valueS +"</span>" );
				}
				else {
					sb.append( "<span class=\"remove\">"+ valueS +"</span><br><span class=\"add\">"+ valueT +"</span>" );
				}
				sb.append("</td>");
			}
		}
		sb.append("</tr>");
		out(sb.toString()+"\n", fos);
	}
	
	List<Object> getFormattedVals(List<Object> vals) {
		List<Object> objs = new ArrayList<Object>();
		for(int i=0;i<lsColNames.size();i++) {
			objs.add( DataDumpUtils.getFormattedXMLValue(vals.get(i), lsColTypes.get(i), floatFormatter, dateFormatter, nullValueStr, doEscape(i)) );
		}
		return objs;
	}
	
	boolean equals(List<Object> valS, List<Object> valT) {
		for(int i=0;i<valS.size();i++) {
			Object s = valS.get(i);
			Object t = valT.get(i);
			if(s==null && t==null) continue;
			if((s==null && t!=null) || (s!=null && t==null)) {
				return false;
			}
			if(!s.equals(t)) {
				return false;
			}
		}
		return true;
	}

	void flush(Writer w) throws IOException {
		//w.flush();
		w.close();
	}

}
