package tbrugz.sqldump.sqlrun;

import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
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

import tbrugz.sqldump.datadump.DataDumpUtils;
import tbrugz.sqldump.datadump.DumpSyntaxInt;
import tbrugz.sqldump.def.AbstractFailable;
import tbrugz.sqldump.sqlrun.def.CommitStrategy;
import tbrugz.sqldump.sqlrun.def.Constants;
import tbrugz.sqldump.sqlrun.def.Executor;
import tbrugz.sqldump.util.CategorizedOut;
import tbrugz.sqldump.util.SQLUtils;
import tbrugz.sqldump.util.Utils;

public class QueryDumper extends AbstractFailable implements Executor {
	static final Log log = LogFactory.getLog(QueryDumper.class);
	
	//suffixes
	static final String SUFFIX_DUMPSYNTAX = ".dumpsyntax";
	static final String SUFFIX_OUTSTREAM = ".outputstream";
	static final String SUFFIX_QUERYNAME = ".queryname";
	
	static final String[] QUERYDUMPER_EXEC_SUFFIXES = {
		SQLRun.SUFFIX_QUERY
	};
	
	static final String[] QUERYDUMPER_AUX_SUFFIXES = {
		SUFFIX_DUMPSYNTAX,
		SUFFIX_OUTSTREAM,
		SUFFIX_QUERYNAME
	};
	
	Connection conn = null;
	String execId = null;
	String queryName = null;
	String outputStream = null;
	DumpSyntaxInt dumpSyntax = null;

	@Override
	public void setExecId(String execId) {
		this.execId = execId;
	}

	@Override
	public void setConnection(Connection conn) {
		this.conn = conn;
	}
	
	@Override
	public void setProperties(Properties prop) {
		//this.prop = prop;
		queryName = prop.getProperty(Constants.PREFIX_EXEC+execId+SUFFIX_QUERYNAME);
		outputStream = prop.getProperty(Constants.PREFIX_EXEC+execId+SUFFIX_OUTSTREAM);
		if(outputStream==null) {
			//XXX: default to <stdout>?
			log.error("output stream (suffix "+SUFFIX_OUTSTREAM+") not defined");
		}
		String dumpSyntaxStr = prop.getProperty(Constants.PREFIX_EXEC+execId+SUFFIX_DUMPSYNTAX);
		if(dumpSyntaxStr==null) {
			//XXX: default to FFCDataDump?
			log.error("dump syntax (suffix "+SUFFIX_DUMPSYNTAX+") not defined");
		}
		else {
			dumpSyntax = getSyntax(dumpSyntaxStr, prop);
			if(dumpSyntax==null) {
				log.error("dump syntax '"+dumpSyntaxStr+"' not found");
			}
		}
	}
	
	//XXX @Override
	public void execQuery(String sql) throws SQLException, IOException {
		long initTime = System.currentTimeMillis();
		
		PreparedStatement st = conn.prepareStatement(sql);
		ResultSet rs = st.executeQuery();
		SQLUtils.setupForNewQuery(rs.getMetaData().getColumnCount());
		// dump column types (debug)
		DataDumpUtils.logResultSetColumnsTypes(rs.getMetaData(), queryName, log);
		
		int count = 0;
		if(dumpSyntax.acceptsOutputStream()) {
			OutputStream os = getOutputStream(outputStream);
			count = dumpResultSet(rs, dumpSyntax, os, queryName, null);
			os.flush();
		}
		else {
			Writer w = getWriter(outputStream);
			count = dumpResultSet(rs, dumpSyntax, w, queryName, null);
			w.flush();
		}
		
		long totalTime = System.currentTimeMillis() - initTime;
		log.info("query '"+execId+"' dumped [lines = "+count+"; elapsed = "+totalTime+"ms]");
	}

	int dumpResultSet(ResultSet rs, DumpSyntaxInt ds, Writer w,
			String queryName, List<String> uniqueColumns)
			throws SQLException, IOException {
		int count = 0;
		
		ds.initDump(null, queryName, uniqueColumns, rs.getMetaData());

		ds.dumpHeader(w);
		boolean hasNext = ds.isFetcherSyntax()?true:rs.next();
		while(hasNext) {
			ds.dumpRow(rs, count, w);
			count++;
			hasNext = rs.next();
		}
		ds.dumpFooter(count, false, w);
		
		return count;
	}

	int dumpResultSet(ResultSet rs, DumpSyntaxInt ds, OutputStream os,
			String queryName, List<String> uniqueColumns)
			throws SQLException, IOException {
		int count = 0;
		
		ds.initDump(null, queryName, uniqueColumns, rs.getMetaData());

		ds.dumpHeader(os);
		boolean hasNext = ds.isFetcherSyntax()?true:rs.next();
		while(hasNext) {
			ds.dumpRow(rs, count, os);
			count++;
			hasNext = rs.next();
		}
		ds.dumpFooter(count, false, os);
		
		return count;
	}
	
	@Override
	public List<String> getAuxSuffixes() {
		List<String> ret = new ArrayList<String>();
		ret.addAll(Arrays.asList(QUERYDUMPER_AUX_SUFFIXES));
		return ret;
	}
	
	static DumpSyntaxInt getSyntax(String className, Properties prop) {
		Class<?> c = Utils.getClassWithinPackages(className, "tbrugz.sqldump.datadump", null);
		if(c==null) {
			log.warn("class '"+className+"' not found");
			return null;
		}
		DumpSyntaxInt ds = (DumpSyntaxInt) Utils.getClassInstance(c);
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

	static OutputStream getOutputStream(String output) throws IOException {
		if(CategorizedOut.STDOUT.equals(output)) {
			return System.out;
		}
		if(CategorizedOut.STDERR.equals(output)) {
			return System.err;
		}
		
		return new FileOutputStream(output);
	}
	
	@Override
	public void setCommitStrategy(CommitStrategy commitStrategy) {
	}

	@Override
	public List<String> getExecSuffixes() {
		List<String> ret = new ArrayList<String>();
		ret.addAll(Arrays.asList(QUERYDUMPER_EXEC_SUFFIXES));
		return ret;
	}
	
	public static void simplerRSDump(ResultSet rs) throws SQLException, IOException {
		Properties p = new Properties();
		p.setProperty("sqldump.datadump.ffc.nullvalue", "-");
		simpleRSDump(rs, "FFCDataDump", p, new PrintWriter(System.out));
	}
	
	public static void simpleRSDump(ResultSet rs, String dumpClass, Properties dumpProp, Writer w) throws SQLException, IOException {
		QueryDumper qd = new QueryDumper();
		DumpSyntaxInt dumpSyntax = getSyntax(dumpClass, dumpProp);
		qd.dumpResultSet(rs, dumpSyntax, w, null, null);
		w.flush();
	}

	@Override
	public void setDefaultFileEncoding(String encoding) {
	} 
}
