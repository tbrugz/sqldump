package tbrugz.sqldiff.model;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.DBObject;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.NamedDBObject;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.def.DBMSFeatures;
import tbrugz.sqldump.def.DBMSResources;

//XXX: rename to ColumnDiff?
//@XmlJavaTypeAdapter(TableColumnDiffAdapter.class)
public class TableColumnDiff implements Diff, Comparable<TableColumnDiff> {
	static final Log log = LogFactory.getLog(TableColumnDiff.class);
	
	public enum TempColumnAlterStrategy {
		//ALWAYS > TYPESDIFFER > NEWPRECISIONSMALLER > NEVER
		NEVER,
		NEWPRECISIONSMALLER,
		TYPESDIFFER,
		ALWAYS;
	}
	
	final ChangeType type; //ADD, ALTER, RENAME, DROP;
	final Table table; //TODO: change to tableName & schemaName?
	final Column column;
	final Column previousColumn;
	
	static boolean addComments = true;
	static DBMSFeatures features;
	public static TempColumnAlterStrategy useTempColumnStrategy = TempColumnAlterStrategy.NEVER;

	public TableColumnDiff(ChangeType changeType, Table table, Column oldColumn, Column newColumn) {
		this.type = changeType;
		this.table = table;
		this.column = newColumn;
		this.previousColumn = oldColumn;
		
		if(features==null) {
			features = DBMSResources.instance().databaseSpecificFeaturesClass();
			log.debug("DBMSFeatures class: "+features);
		}
	}
	
	@Override
	public String getDiff() {
		return getDiff(type, previousColumn, column);
	}
	
	public String getDiff(ChangeType changeType, Column previousColumn, Column column) {
		String colChange = null;
		switch(changeType) {
			case ADD:
				colChange = features.sqlAddColumnClause()+" "+Column.getColumnDesc(column); break; //COLUMN "+column.name+" "+column.type;
			case ALTER:
				return getAlterColumn(); //XXX beware of recursion...
			case RENAME:
				//colChange = "rename column "+(previousColumn!=null?previousColumn.getName():"[unknown]")+" TO "+column.getName(); break;
				colChange = "rename column "+DBObject.getFinalIdentifier(previousColumn.getName())
					+" TO "+DBObject.getFinalIdentifier(column.getName());
				break;
			case DROP:
				colChange = "drop column "+DBObject.getFinalIdentifier(previousColumn.getName());
				break;
		}
		return "alter table "+DBObject.getFinalName(table, true)+" "+colChange;
	}
	
	String getAlterColumn() {
		switch(useTempColumnStrategy) {
		case ALWAYS: break;
		case TYPESDIFFER:
			//if(! column.type.equals(previousColumn.type)) break;
		case NEWPRECISIONSMALLER:
			if(! column.type.equals(previousColumn.type)) break;
			if(useTempColumnStrategy==TempColumnAlterStrategy.NEWPRECISIONSMALLER
				&& column.columSize!=null && previousColumn.columSize!=null
				&& previousColumn.columSize>column.columSize) break;
		case NEVER:
			String colChange = features.sqlAlterColumnClause()+" "+Column.getColumnDesc(column);
			if(addComments) {
				colChange += " /* from: "+Column.getColumnDesc(previousColumn)+" */";
			}
			return colChange;
		}
		
		//rename old to temp, create new, update new from old, drop temp
		Column tmpColumn = previousColumn.clone();
		tmpColumn.setName(tmpColumn.getName()+"_TMP");
		String ret = getDiff(ChangeType.RENAME, previousColumn, tmpColumn)+";\n"+
				getDiff(ChangeType.ADD, null, column)+";\n"+
				"update "+DBObject.getFinalName(table, true)+" set "+column.getName()+" = "+tmpColumn.getName()+";\n"+
				getDiff(ChangeType.DROP, tmpColumn, null)+
				(addComments?" /* from: "+Column.getColumnDesc(previousColumn)+" */":"");
		return ret;
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
	public int compareTo(TableColumnDiff o) {
		int comp = type.compareTo(o.type);
		if(comp==0) { comp = table.compareTo(o.table); }
		if(comp==0) {
			if(column!=null && o.column!=null) {
				comp = column.getName().compareTo(o.column.getName());
			}
		}
		return comp;
	}
	
	@Override
	public String toString() {
		return "[ColDiff:"+table.getQualifiedName()+","+type+","+column+"]";
	}

	@Override
	public ChangeType getChangeType() {
		return type;
	}

	@Override
	public DBObjectType getObjectType() {
		return DBObjectType.COLUMN;
	}
	
	@Override
	public NamedDBObject getNamedObject() {
		return table;
	}
	
	@Override
	public TableColumnDiff inverse() {
		return new TableColumnDiff(type.inverse(), table, column, previousColumn);
	}

}
