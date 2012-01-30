package tbrugz.sqldiff.model;

import tbrugz.sqldiff.ChangeType;
import tbrugz.sqldiff.Diff;
import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.DBObject;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.Table;

public class TableColumnDiff extends DBObject implements Diff {
	ChangeType type; //ADD, ALTER, RENAME, DROP;
	Column column;
	
	//XXX: instead of renameFrom, add "Column from, to"?
	String renameFrom;

	public TableColumnDiff(ChangeType changeType, Table table, Column newColumn) {
		this.type = changeType;
		this.name = table.name;
		this.setSchemaName(table.getSchemaName());
		this.column = newColumn;
	}
	
	@Override
	public String getDiff() {
		String colChange = null;
		switch(type) {
			case ADD:
				colChange = "ADD COLUMN "+Column.getColumnDesc(column); break; //COLUMN "+column.name+" "+column.type;
			case ALTER:
				//XXX: option: rename old, create new, update new from old, drop old
				colChange = "ALTER COLUMN "+Column.getColumnDesc(column); break; //COLUMN "+column.name+" "+column.type; break;
			case RENAME:
				colChange = "RENAME COLUMN "+renameFrom+" TO "+column.getName(); break;
			case DROP:
				colChange = "DROP COLUMN "+column.getName(); break;
		}
		return "ALTER TABLE "+(getSchemaName()!=null?getSchemaName()+".":"")+name+" "+colChange;
	}

	@Override
	public String getDefinition(boolean dumpSchemaName) {
		return "ALTER TABLE "+(dumpSchemaName?getSchemaName()+".":"")+name
				+getDiff();
	}
	
	/*public static List<TableColumnDiff> tableDiffs(Table origTable, Table newTable) {
		List<TableColumnDiff> diff = new ArrayList<TableColumnDiff>();
		//TODOxx: for each column...
		return diff;
	}*/
	
	@Override
	public boolean equals(Object obj) {
		if(super.equals(obj)) {
			if(obj instanceof TableColumnDiff) {
				TableColumnDiff tcd = (TableColumnDiff) obj;
				if(type.equals(tcd.type)) {
					return column.equals(tcd.column);
				}
			}
		}
		return false;
	}
	
	@Override
	public int compareTo(DBObject o) {
		int comp = super.compareTo(o);
		if(comp==0) {
			if(o instanceof TableColumnDiff) {
				TableColumnDiff tcd = (TableColumnDiff) o;
				comp = type.compareTo(tcd.type);
				if(comp==0) {
					return column.getName().compareTo(tcd.column.getName());
				}
			}
		}
		return comp;
	}
	
	@Override
	public String toString() {
		return "[ColDiff:"+name+","+type+","+column+"]";
	}

	@Override
	public ChangeType getChangeType() {
		return type;
	}

	@Override
	public DBObjectType getObjectType() {
		return DBObjectType.COLUMN;
	}

}
