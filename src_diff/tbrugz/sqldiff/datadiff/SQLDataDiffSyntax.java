package tbrugz.sqldiff.datadiff;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import tbrugz.sqldump.SQLUtils;
import tbrugz.sqldump.datadump.DataDumpUtils;
import tbrugz.sqldump.datadump.UpdateByPKDataDump;
import tbrugz.sqldump.util.Utils;

public class SQLDataDiffSyntax extends UpdateByPKDataDump implements DiffSyntax {

	@Override
	public void dumpRow(ResultSet rs, long count, Writer fos)
			throws IOException, SQLException {
		superDumpRow(rs, count, fos);
	}
	
	@Override
	public boolean dumpUpdateRowIfNotEquals(ResultSet rsSource,
			ResultSet rsTarget, long count, Writer w) throws IOException,
			SQLException {
		// TODO compare!
		dumpUpdateRowInternal(rsTarget, null, count, w);
		return true;
	}

	@Override
	public void dumpUpdateRow(ResultSet rsSource, ResultSet rsTarget,
			long count, Writer w) throws IOException, SQLException {
		dumpUpdateRowInternal(rsTarget, null, count, w);
	}

	@Override
	public void dumpDeleteRow(ResultSet rs, long count, Writer w)
			throws IOException, SQLException {
		List<String> vals = (List<String>) DataDumpUtils.values4sql( SQLUtils.getRowObjectListFromRS(rs, lsColTypes, numCol), dateFormatter );
		
		List<String> wheres = new ArrayList<String>();
		
		for(int i = 0;i<lsColNames.size();i++) {
			String colname = lsColNames.get(i);
			if(pkCols.contains(colname)) {
				wheres.add(colname+" = "+vals.get(i));
			}
		}
		
		out("delete from "+tableName+
				" where "+
				Utils.join(wheres, " and ")+
				";", w);
	}

	void dumpUpdateRowInternal(ResultSet rs, List<String> colsToUpdate, long count, Writer w) throws IOException, SQLException {
		List<String> vals = (List<String>) DataDumpUtils.values4sql( SQLUtils.getRowObjectListFromRS(rs, lsColTypes, numCol), dateFormatter );
		
		List<String> sets = new ArrayList<String>();
		List<String> wheres = new ArrayList<String>();
		
		for(int i = 0;i<lsColNames.size();i++) {
			String colname = lsColNames.get(i);
			if(!pkCols.contains(colname)) {
				sets.add(colname+" = "+vals.get(i));
			}
			else if(colsToUpdate==null || colsToUpdate.contains(colname)) {
				wheres.add(colname+" = "+vals.get(i));
			}
		}
		
		out("update "+tableName+" set "+
				Utils.join(sets, ", ")+
				" where "+
				Utils.join(wheres, " and ")+
				";", w);
	}	
	
}
