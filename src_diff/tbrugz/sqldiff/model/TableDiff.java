package tbrugz.sqldiff.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import tbrugz.sqldiff.ChangeType;
import tbrugz.sqldiff.Diff;
import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.Table;

public class TableDiff implements Diff, Comparable<TableDiff> {
	static Logger log = Logger.getLogger(TableDiff.class);

	ChangeType diffType; //ADD, ALTER, RENAME, DROP;
	String renameFrom;
	Table table;

	public TableDiff(ChangeType changeType, Table table) {
		this.diffType = changeType;
		this.table = table;
	}
	
	@Override
	public String getDiff() {
		switch(diffType) {
			case ADD:
				return table.getDefinition(true, true, false, null, null); //XXX: is it useful?
				//return getDefinition(dumpWithSchemaName, doSchemaDumpPKs, dumpFKsInsideTable, colTypeConversionProp, foreignKeys);
			case ALTER:
				return null; //XXX: alter table...??
			case RENAME:
				return "ALTER TABLE "+renameFrom+" RENAME TO "+table.name+";";
			case DROP:
				return "DROP table "+table.name+";";
		}
		return null;
	}
	
	public static List<Diff> tableDiffs(Table origTable, Table newTable) {
		//log.debug("difftable:\n"+origTable.getDefinition(true)+"\n"+newTable.getDefinition(true));
		List<Diff> diffs = new ArrayList<Diff>();
		if(!origTable.getName().equals(newTable.getName())) {
			TableDiff td = new TableDiff(ChangeType.RENAME, newTable);
			//td.table = newTable;
			//td.diffType = ChangeType.RENAME;
			diffs.add(td);
		}
		Set<Column> newColumnsThatExistsInOrigModel = new HashSet<Column>();
		for(Column cOrig: origTable.getColumns()) {
			Column cNew = getColumnByName(newTable.getColumns(), cOrig.name);
			if(cNew!=null) {
				newColumnsThatExistsInOrigModel.add(cNew);
				boolean equal = cNew.equals(cOrig);
				if(!equal) {
					//alter column
					log.debug("alter column: orig: "+cOrig+" new: "+cNew);
					TableColumnDiff tcd = new TableColumnDiff(ChangeType.ALTER, newTable, cNew);
					diffs.add(tcd);
				}
				//else {
					//log.info("equal columns: cOrig: "+cOrig+", cNew: "+cNew);
				//}
			}
			else {
				log.info("drop column: orig: "+cOrig);
				TableColumnDiff tcd = new TableColumnDiff(ChangeType.DROP, origTable, cOrig);
				diffs.add(tcd);
			}
		}
		for(Column cNew: newTable.getColumns()) {
			if(newColumnsThatExistsInOrigModel.contains(cNew)) { continue; }
			log.info("add column: new: "+cNew);
			TableColumnDiff tcd = new TableColumnDiff(ChangeType.ADD, newTable, cNew);
			diffs.add(tcd);
		}
		return diffs;
	}
	
	static Column getColumnByName(Collection<Column> cols, String colName) {
		for(Column col: cols) {
			if(col.name.equals(colName)) return col;
		}
		return null;
	}

	@Override
	public int compareTo(TableDiff o) {
		int comp = diffType.compareTo(o.diffType);
		if(comp==0) { return table.compareTo(o.table); }
		return comp;
	}

	@Override
	public ChangeType getChangeType() {
		return diffType;
	}

}
