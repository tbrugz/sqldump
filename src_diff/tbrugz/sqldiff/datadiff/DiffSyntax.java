package tbrugz.sqldiff.datadiff;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;

import tbrugz.sqldump.datadump.DumpSyntaxInt;

//XXX: rename to DataDiffSyntax ?
public interface DiffSyntax extends DumpSyntaxInt {
	
	public boolean dumpUpdateRowIfNotEquals(ResultSet rsSource, ResultSet rsTarget, long count, boolean alsoDumpIfEquals, Writer w)
			throws IOException, SQLException;
	
	public void dumpUpdateRow(ResultSet rsSource, ResultSet rsTarget, long count, Writer w)
			throws IOException, SQLException;

	public void dumpDeleteRow(ResultSet rs, long count, Writer w)
			throws IOException, SQLException;
	
	public void dumpStats(long insertCount, long updateCount, long deleteCount, long identicalRowsCount,
			long sourceRowCount, long targetRowCount, Writer w) throws IOException, SQLException;
	
	//XXX add public boolean supportsEqualsDump(); //??
	
}
