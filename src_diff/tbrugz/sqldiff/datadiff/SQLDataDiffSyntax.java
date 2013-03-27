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

	boolean shouldFlush = true;
	
	@Override
	public void dumpRow(ResultSet rs, long count, Writer fos)
			throws IOException, SQLException {
		superDumpRow(rs, count, fos);
		if(shouldFlush) { fos.flush(); }
	}
	
	//XXX: option to select update strategy: updatealways/if modified, update changed cols/all cols ?
	@Override
	public boolean dumpUpdateRowIfNotEquals(ResultSet rsSource,
			ResultSet rsTarget, long count, Writer w) throws IOException,
			SQLException {
		List<String> valsS = (List<String>) DataDumpUtils.values4sql( SQLUtils.getRowObjectListFromRS(rsSource, lsColTypes, numCol), dateFormatter );
		List<String> valsT = (List<String>) DataDumpUtils.values4sql( SQLUtils.getRowObjectListFromRS(rsTarget, lsColTypes, numCol), dateFormatter );
		
		List<String> changedCols = getChangedCols(lsColNames, valsS, valsT);
		if(changedCols.size()>0) {
			//dumpUpdateRowInternal(rsTarget, null, count, w); //updates all cols
			dumpUpdateRowInternal(rsTarget, changedCols, count, w); //updates changed cols
			return true;
		}
		else {
			return false;
		}
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
		if(shouldFlush) { w.flush(); }
	}

	void dumpUpdateRowInternal(ResultSet rs, List<String> colsToUpdate, long count, Writer w) throws IOException, SQLException {
		List<String> vals = (List<String>) DataDumpUtils.values4sql( SQLUtils.getRowObjectListFromRS(rs, lsColTypes, numCol), dateFormatter );
		
		List<String> sets = new ArrayList<String>();
		List<String> wheres = new ArrayList<String>();
		
		for(int i = 0;i<lsColNames.size();i++) {
			String colname = lsColNames.get(i);
			if(!pkCols.contains(colname)) {
				if(colsToUpdate==null || colsToUpdate.contains(colname)) {
					sets.add(colname+" = "+vals.get(i));
				}
			}
			else {
				wheres.add(colname+" = "+vals.get(i));
			}
		}
		
		out("update "+tableName+" set "+
				Utils.join(sets, ", ")+
				" where "+
				Utils.join(wheres, " and ")+
				";", w);
		if(shouldFlush) { w.flush(); }
	}
	
	static List<String> getChangedCols(List<String> lsColNames, List<String> vals1, List<String> vals2) {
		int comp = 0;
		int size = vals1.size();
		List<String> changedColNames = new ArrayList<String>();
		for(int i=0;i<size; i++) {
			comp = vals1.get(i).compareTo(vals2.get(i));
			if(comp!=0) {
				changedColNames.add(lsColNames.get(i));
			}
		}
		return changedColNames;
	}
	
}
