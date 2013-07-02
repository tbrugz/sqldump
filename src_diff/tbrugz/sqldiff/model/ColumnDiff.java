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

//@XmlJavaTypeAdapter(TableColumnDiffAdapter.class)
public class ColumnDiff implements Diff, Comparable<ColumnDiff> {
	static final Log log = LogFactory.getLog(ColumnDiff.class);
	
	public enum TempColumnAlterStrategy {
		//ALWAYS > TYPESDIFFER > NEWPRECISIONSMALLER > NEVER
		NEVER,
		NEWPRECISIONSMALLER,
		TYPESDIFFER,
		ALWAYS;
	}
	
	class NamedTable implements NamedDBObject {
		@Override
		public String getName() {
			return tableName;
		}
		
		@Override
		public String getSchemaName() {
			return schemaName;
		}
		
		public int compareTo(NamedDBObject o) {
			int comp = schemaName!=null?schemaName.compareTo(o.getSchemaName()):o.getSchemaName()!=null?1:0; //XXX: return -1? 1?
			if(comp!=0) return comp;
			return tableName.compareTo(o.getName());
		}
	}
	
	final ChangeType type; //ADD, ALTER, RENAME, DROP;
	final String schemaName;
	final String tableName;
	final Column column;
	final Column previousColumn;
	final transient NamedTable table;
	
	static boolean addComments = true;
	static DBMSFeatures features;
	public static TempColumnAlterStrategy useTempColumnStrategy = TempColumnAlterStrategy.NEVER;

	public ColumnDiff(ChangeType changeType, Table table, Column oldColumn, Column newColumn) {
		this(changeType, table.getSchemaName(), table.getName(), oldColumn, newColumn);
	}

	public ColumnDiff(ChangeType changeType, String schemaName, String tableName, Column oldColumn, Column newColumn) {
		this.type = changeType;
		this.schemaName = schemaName;
		this.tableName = tableName;
		this.column = newColumn;
		this.previousColumn = oldColumn;
		this.table = new NamedTable();
		
		if(features==null) {
			features = DBMSResources.instance().databaseSpecificFeaturesClass();
			log.debug("DBMSFeatures class: "+features);
		}
	}
	
	@Override
	public String getDiff() {
		return getDiff(type, previousColumn, column);
	}
	
	//XXX: return List<String>? - in case of multiple statements for 1 change
	String getDiff(ChangeType changeType, Column previousColumn, Column column) {
		String colChange = null;
		switch(changeType) {
			case ADD:
				colChange = features.sqlAddColumnClause()+" "+Column.getColumnDesc(column); break; //COLUMN "+column.name+" "+column.type;
			case ALTER:
				return getAlterColumn(); //XXX beware of recursion...
			case RENAME:
				return features.sqlRenameColumnDefinition(table, previousColumn, column.getName());
				//colChange = "rename column "+(previousColumn!=null?previousColumn.getName():"[unknown]")+" TO "+column.getName(); break;
				/*colChange = features.sqlAlterColumnClause()+" "+DBObject.getFinalIdentifier(previousColumn.getName())
					+" rename to "+DBObject.getFinalIdentifier(column.getName());
				break;*/
			case DROP:
				colChange = "drop column "+DBObject.getFinalIdentifier(previousColumn.getName());
				break;
		}
		return "alter table "+DBObject.getFinalName(table, true)+" "+colChange;
	}

	//XXX: return List<String>?
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
			return "alter table "+DBObject.getFinalName(table, true)+" "+colChange; //refactor...
		}
		
		//rename old to temp, create new, update new from old, drop temp
		//- option2: add temp, update temp from old, drop old, rename temp to new 
		//XXX what if column is not null & table has data?
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
			if(obj instanceof ColumnDiff) {
				ColumnDiff tcd = (ColumnDiff) obj;
				if(type.equals(tcd.type)) {
					return column.equals(tcd.column);
				}
			}
		}
		return false;
	}
	
	@Override
	public int compareTo(ColumnDiff o) {
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
		return "[ColDiff:"+DBObject.getFinalName(table, true)+","+type+","+column+"]";
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
	public ColumnDiff inverse() {
		return new ColumnDiff(type.inverse(), schemaName, tableName, column, previousColumn);
	}

}
