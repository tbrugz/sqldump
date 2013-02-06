package tbrugz.sqldiff.model;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.DBObject;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.def.DBMSFeatures;
import tbrugz.sqldump.def.DBMSResources;

public class TableColumnDiff extends DBObject implements Diff {
	private static final long serialVersionUID = 1L;
	static Log log = LogFactory.getLog(TableColumnDiff.class);
	
	final ChangeType type; //ADD, ALTER, RENAME, DROP;
	final Column column;
	final Column previousColumn;
	
	static boolean addComments = true;
	static DBMSFeatures features;

	public TableColumnDiff(ChangeType changeType, Table table, Column oldColumn, Column newColumn) {
		this.type = changeType;
		this.setName(table.getName());
		this.setSchemaName(table.getSchemaName());
		this.column = newColumn;
		this.previousColumn = oldColumn;
		
		if(features==null) {
			features = DBMSResources.instance().databaseSpecificFeaturesClass();
			log.debug("DBMSFeatures class: "+features);
		}
	}
	
	@Override
	public String getDiff() {
		String colChange = null;
		switch(type) {
			case ADD:
				colChange = "add column "+Column.getColumnDesc(column); break; //COLUMN "+column.name+" "+column.type;
			case ALTER:
				//XXX: option: rename old, create new, update new from old, drop old
				colChange = features.sqlAlterColumnClause()+" "+Column.getColumnDesc(column);
				if(addComments) {
					colChange += " /* from: "+Column.getColumnDesc(previousColumn)+" */";
				}
				break; //COLUMN "+column.name+" "+column.type; break;
			case RENAME:
				//colChange = "rename column "+(previousColumn!=null?previousColumn.getName():"[unknown]")+" TO "+column.getName(); break;
				colChange = "rename column "+DBObject.getFinalIdentifier(previousColumn.getName())
					+" TO "+DBObject.getFinalIdentifier(column.getName());
				break;
			case DROP:
				colChange = "drop column "+DBObject.getFinalIdentifier(previousColumn.getName());
				break;
		}
		return "alter table "+DBObject.getFinalQualifiedName(this, true)+" "+colChange;
	}

	@Override
	public String getDefinition(boolean dumpSchemaName) {
		return "alter table "+DBObject.getFinalQualifiedName(this, dumpSchemaName)
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
					if(column==null || tcd.column==null) return 0;
					return column.getName().compareTo(tcd.column.getName());
				}
			}
		}
		return comp;
	}
	
	@Override
	public String toString() {
		return "[ColDiff:"+getName()+","+type+","+column+"]";
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
