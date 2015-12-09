package tbrugz.sqldiff.model;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldiff.util.DiffUtil;
import tbrugz.sqldump.dbmd.DBMSFeatures;
import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.DBObject;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.NamedDBObject;
import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.def.DBMSUpdateListener;
import tbrugz.sqldump.util.Utils;

//@XmlJavaTypeAdapter(TableColumnDiffAdapter.class)
public class ColumnDiff implements Diff, Comparable<ColumnDiff> {
	static final Log log = LogFactory.getLog(ColumnDiff.class);
	
	public enum TempColumnAlterStrategy {
		//ALWAYS > NEWPRECISIONSMALLER > TYPESDIFFER > NEVER
		NEVER,
		TYPESDIFFER,
		NEWPRECISIONSMALLER,
		ALWAYS;
	}
	
	public static class NamedTable implements NamedDBObject {
		final String schemaName, tableName;
		
		public NamedTable(String schemaName, String tableName) {
			this.schemaName = schemaName;
			this.tableName = tableName;
		}
		
		@Override
		public String getName() {
			return tableName;
		}
		
		@Override
		public String getSchemaName() {
			return schemaName;
		}
		
		@Override
		public String toString() {
			return "NamedTable:"+(schemaName!=null?schemaName+".":"")+tableName;
		}
		
		public int compareTo(NamedDBObject o) {
			int comp = schemaName!=null?schemaName.compareTo(o.getSchemaName()):o.getSchemaName()!=null?1:0; //XXX: return -1? 1?
			if(comp!=0) return comp;
			return tableName.compareTo(o.getName());
		}
		
		@Override
		public boolean equals(Object obj) {
			if(obj==null) { return false; }
			if(! (obj instanceof NamedDBObject)) { return false; }
			return compareTo((NamedDBObject) obj)==0;
		}
		
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
			result = prime * result + ((schemaName == null) ? 0 : schemaName.hashCode());
			return result;
		}
	}
	
	static final DBMSUpdateListener updateListener = new DBMSUpdateListener() {
		@Override
		public void dbmsUpdated() {
			updateFeatures(null);
			log.debug("DBMSUpdateListener: DBMSFeatures class: "+features);
		}
	};
	
	static {
		DBMSResources.instance().addUpdateListener(updateListener);
	}
	
	final ChangeType type; //ADD, ALTER, RENAME, DROP;
	final String schemaName;
	final String tableName;
	final Column column;
	final Column previousColumn;
	final transient NamedTable table;
	
	public static boolean addComments = true;
	static DBMSFeatures features;
	public static TempColumnAlterStrategy useTempColumnStrategy = TempColumnAlterStrategy.NEVER;

	public ColumnDiff(ChangeType changeType, NamedDBObject table, Column oldColumn, Column newColumn) {
		this(changeType, table.getSchemaName(), table.getName(), oldColumn, newColumn);
	}

	ColumnDiff(ChangeType changeType, String schemaName, String tableName, Column oldColumn, Column newColumn) {
		this.type = changeType;
		this.schemaName = schemaName;
		this.tableName = tableName;
		this.column = newColumn;
		this.previousColumn = oldColumn;
		this.table = new NamedTable(schemaName, tableName);
		
		if(features==null) {
			updateFeatures(null);
			log.debug("DBMSFeatures class: "+features);
		}
	}
	
	public static void updateFeatures(DBMSFeatures feat) {
		log.debug("updateFeatures: feat="+feat+" [old-feat="+features+"]");
		if(feat!=null) {
			features = feat;
		}
		else {
			features = DBMSResources.instance().databaseSpecificFeaturesClass();
		}
	}
	
	@Override
	public String getDiff() {
		return Utils.join(getDiffList(), ";\n");
	}

	@Override
	public List<String> getDiffList() {
		return getDiff(type, previousColumn, column);
	}
	
	List<String> getDiff(ChangeType changeType, Column previousColumn, Column column) {
		String colChange = null;
		switch(changeType) {
			case ADD:
				colChange = features.sqlAddColumnClause()+" "+column.getDefinition(); break; //COLUMN "+column.name+" "+column.type;
			case ALTER:
				return getAlterColumn(); //XXX beware of recursion...
			case RENAME:
				return DiffUtil.singleElemList( features.sqlRenameColumnDefinition(table, previousColumn, column.getName()) );
				//colChange = "rename column "+(previousColumn!=null?previousColumn.getName():"[unknown]")+" TO "+column.getName(); break;
				/*colChange = features.sqlAlterColumnClause()+" "+DBObject.getFinalIdentifier(previousColumn.getName())
					+" rename to "+DBObject.getFinalIdentifier(column.getName());
				break;*/
			case DROP:
				colChange = "drop column "+DBObject.getFinalIdentifier(previousColumn.getName());
				break;
			case REPLACE:
				throw new IllegalArgumentException("illegal ChangeType for ColumnDiff: "+changeType);
		}
		return DiffUtil.singleElemList( "alter table "+DBObject.getFinalName(table, true)+" "+colChange );
	}

	List<String> getAlterColumn() {
		List<String> ret = new ArrayList<String>();
		
		switch(useTempColumnStrategy) {
		case ALWAYS: break;
		case NEWPRECISIONSMALLER:
			if(column.getColumSize()!=null && previousColumn.getColumSize()!=null
				&& previousColumn.getColumSize()>column.getColumSize()) break;
		case TYPESDIFFER:
			if(! column.getType().equals(previousColumn.getType())) break;
			//if(! column.type.equals(previousColumn.type)) break;
		case NEVER:
			ret.add(getAlterColumnSQL());
			return ret;
		}
		
		//rename old to temp, create new, update new from old, drop temp
		//- option2: add temp, update temp from old, drop old, rename temp to new 
		//XXX what if column is not null & table has data?
		Column tmpColumn = previousColumn.clone();
		tmpColumn.setName(tmpColumn.getName()+"_TMP");
		
		boolean columnNotNull = !column.isNullable();
		ret.add( getDiff(ChangeType.RENAME, previousColumn, tmpColumn).get(0) );
		if(columnNotNull) { column.setNullable(true); }
		ret.add( getDiff(ChangeType.ADD, null, column).get(0) );
		ret.add( "update "+DBObject.getFinalName(table, true)+" set "+column.getName()+" = "+tmpColumn.getName() );
		if(columnNotNull) { column.setNullable(false); ret.add(getAlterColumnSQL()); }
		ret.add( getDiff(ChangeType.DROP, tmpColumn, null).get(0)+
				(addComments?" /* from: "+previousColumn.getDefinition()+" */":"") );
		return ret;
	}
	
	String getAlterColumnSQL() {
		String alterSql = null;
		if(features.supportsDiffingColumn()) {
			alterSql = features.sqlAlterColumnByDiffing(table, previousColumn, column);
		}
		else {
			//XXX: return all diffs in one query? return List<String>?
			//oracle-like syntax?
			alterSql = "alter table "+DBObject.getFinalName(table, true)+" "+features.sqlAlterColumnClause()+" "+column.getName();
			if(!previousColumn.getTypeDefinition().equals(column.getTypeDefinition())) {
				alterSql += " "+column.getTypeDefinition();
			}
			else if(!previousColumn.getDefaultSnippet().equals(column.getDefaultSnippet())) {
				alterSql += (column.getDefaultSnippet().trim().equals("")?" default null":column.getDefaultSnippet());
			}
			else if(!previousColumn.getNullableSnippet().equals(column.getNullableSnippet())) {
				alterSql += (column.isNullable()?" null":column.getNullableSnippet());
			}
		}
		
		if(addComments) {
			alterSql += " /* from: "+previousColumn.getDefinition()+" */";
		}
		return alterSql;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((column == null) ? 0 : column.hashCode());
		result = prime * result
				+ ((previousColumn == null) ? 0 : previousColumn.hashCode());
		result = prime * result
				+ ((schemaName == null) ? 0 : schemaName.hashCode());
		result = prime * result
				+ ((tableName == null) ? 0 : tableName.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ColumnDiff other = (ColumnDiff) obj;
		if (column == null) {
			if (other.column != null)
				return false;
		} else if (!column.equals(other.column))
			return false;
		if (previousColumn == null) {
			if (other.previousColumn != null)
				return false;
		} else if (!previousColumn.equals(other.previousColumn))
			return false;
		if (schemaName == null) {
			if (other.schemaName != null)
				return false;
		} else if (!schemaName.equals(other.schemaName))
			return false;
		if (tableName == null) {
			if (other.tableName != null)
				return false;
		} else if (!tableName.equals(other.tableName))
			return false;
		if (type != other.type)
			return false;
		return true;
	}

	@Override
	public int compareTo(ColumnDiff o) {
		int comp = type.compareTo(o.type);
		if(comp==0) { comp = table.compareTo(o.table); }
		if(comp==0 && column!=null && o.column!=null) {
			comp = column.getName().compareTo(o.column.getName());
		}
		if(comp==0 && previousColumn!=null && o.previousColumn!=null) {
			comp = previousColumn.getName().compareTo(o.previousColumn.getName());
		}
		return comp;
	}
	
	@Override
	public String toString() {
		return "[ColDiff:"+DBObject.getFinalName(table, true)+","+type+","
				+(type==ChangeType.RENAME?previousColumn+"->"+column:
					type==ChangeType.DROP?previousColumn:
						column)
				+"]";
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
	
	public Column getColumn() {
		return column;
	}

	public Column getPreviousColumn() {
		return previousColumn;
	}
	
	@Override
	public String getDefinition() {
		return column!=null?column.getDefinition():"";
	}
	
	@Override
	public String getPreviousDefinition() {
		return previousColumn!=null?previousColumn.getDefinition():"";
	}

}
