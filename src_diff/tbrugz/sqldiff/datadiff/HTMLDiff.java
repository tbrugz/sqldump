package tbrugz.sqldiff.datadiff;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.datadump.DataDumpUtils;
import tbrugz.sqldump.datadump.HTMLDataDump;
import tbrugz.sqldump.util.SQLUtils;

public class HTMLDiff extends HTMLDataDump implements DiffSyntax {

	static final Log log = LogFactory.getLog(HTMLDiff.class);
	
	boolean shouldFlush = true;
	
	@Override
	public boolean dumpUpdateRowIfNotEquals(ResultSet rsSource,
			ResultSet rsTarget, long count, Writer w) throws IOException,
			SQLException {
		List<String> valsS = (List<String>) DataDumpUtils.values4sql( SQLUtils.getRowObjectListFromRS(rsSource, lsColTypes, numCol), dateFormatter );
		List<String> valsT = (List<String>) DataDumpUtils.values4sql( SQLUtils.getRowObjectListFromRS(rsTarget, lsColTypes, numCol), dateFormatter );
		
		List<String> changedCols = SQLDataDiffSyntax.getChangedCols(lsColNames, valsS, valsT);
		if(changedCols.size()>0) {
			dumpRow(rsTarget, count, "change", w);
			if(shouldFlush) { w.flush(); }
			return true;
		}
		else {
			dumpRow(rsTarget, count, "equal", w);
			if(shouldFlush) { w.flush(); }
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
		if(shouldFlush) { w.flush(); }
	}

	@Override
	public void dumpDeleteRow(ResultSet rs, long count, Writer w) throws IOException, SQLException {
		dumpRow(rs, count, "remove", w);
		//log.info("dumpDelete: count="+count);
		if(shouldFlush) { w.flush(); }
	}

}
