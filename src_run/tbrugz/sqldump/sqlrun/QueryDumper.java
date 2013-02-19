package tbrugz.sqldump.sqlrun;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.datadump.DumpSyntax;
import tbrugz.sqldump.util.CategorizedOut;
import tbrugz.sqldump.util.Utils;

public class QueryDumper {
	static final Log log = LogFactory.getLog(QueryDumper.class);
	
	//suffixes
	static final String SUFFIX_DUMPSYNTAX = ".dumpsyntax";
	static final String SUFFIX_OUTSTREAM = ".outputstream";
	static final String SUFFIX_QUERYNAME = ".queryname";
	
	static final String[] QUERYDUMPER_AUX_SUFFIXES = {
		SUFFIX_DUMPSYNTAX,
		SUFFIX_OUTSTREAM,
		SUFFIX_QUERYNAME
	};
	
	Connection conn = null;
	String execId = null;
	String queryName = null;
	String outputStream = null;
	DumpSyntax dumpSyntax = null;

	public void setExecId(String execId) {
		this.execId = execId;
	}

	public void setConnection(Connection conn) {
		this.conn = conn;
	}
	
	public void setProperties(Properties prop) {
		//this.prop = prop;
		queryName = prop.getProperty(SQLRun.PREFIX_EXEC+execId+SUFFIX_QUERYNAME);
		outputStream = prop.getProperty(SQLRun.PREFIX_EXEC+execId+SUFFIX_OUTSTREAM);
		if(outputStream==null) {
			log.error("output stream (suffix "+SUFFIX_OUTSTREAM+") not defined");
		}
		String dumpSyntaxStr = prop.getProperty(SQLRun.PREFIX_EXEC+execId+SUFFIX_DUMPSYNTAX);
		if(dumpSyntaxStr==null) {
			log.error("dump syntax (suffix "+SUFFIX_DUMPSYNTAX+") not defined");
		}
		else {
			dumpSyntax = getSyntax(dumpSyntaxStr, prop);
			if(dumpSyntax==null) {
				log.error("dump syntax '"+dumpSyntaxStr+"' not found");
			}
		}
	}
	
	public void execQuery(String sql) throws SQLException, IOException {
		PreparedStatement st = conn.prepareStatement(sql);
		ResultSet rs = st.executeQuery();
		Writer w = getWriter(outputStream);
		dumpResultSet(rs, dumpSyntax, w, queryName, null);
		w.flush();
		//w.close();
	}

	void dumpResultSet(ResultSet rs, DumpSyntax ds, Writer w,
			String queryName, List<String> uniqueColumns)
			throws SQLException, IOException {
		int count = 0;
		
		ds.initDump(queryName, uniqueColumns, rs.getMetaData());

		ds.dumpHeader(w);
		while(rs.next()) {
			ds.dumpRow(rs, count, w);
			count++;
		}
		ds.dumpFooter(w);
	}
	
	//@Override
	public List<String> getAuxSuffixes() {
		List<String> ret = new ArrayList<String>();
		ret.addAll(Arrays.asList(QUERYDUMPER_AUX_SUFFIXES));
		return ret;
	}
	
	static DumpSyntax getSyntax(String className, Properties prop) {
		Class<?> c = Utils.getClassWithinPackages(className, "tbrugz.sqldump.datadump", null);
		if(c==null) {
			log.warn("class '"+className+"' not found");
			return null;
		}
		DumpSyntax ds = (DumpSyntax) Utils.getClassInstance(c);
		if(ds==null) {
			return null;
		}
		ds.procProperties(prop);
		return ds;
	}
	
	static Writer getWriter(String writerName) throws IOException {
		if(CategorizedOut.STDOUT.equals(writerName)) {
			return new PrintWriter(System.out);
		}
		if(CategorizedOut.STDERR.equals(writerName)) {
			return new PrintWriter(System.err);
		}
		
		return new FileWriter(writerName);
	}
	
}
