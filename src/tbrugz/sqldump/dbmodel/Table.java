package tbrugz.sqldump.dbmodel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import tbrugz.sqldump.SQLDump;
import tbrugz.sqldump.Utils;

public class Table extends DBObject {
	public TableType type;
	public List<Column> columns = new ArrayList<Column>();
	public List<Grant> grants = new ArrayList<Grant>();
	public String pkConstraintName;
	
	public Column getColumn(String name) {
		if(name==null) return null;
		for(Column c: columns) {
			if(name.equals(c.name)) return c;
		}
		return null;
	}
	
	@Override
	public String toString() {
		return type+":"+name;
		//return "t:"+name;
		//return "Table[name:"+name+"]";
	}

	@Override
	public String getDefinition(boolean dumpSchemaName) {
		// XXX Table: getDefinition
		return null;
	}
	
	public String getDefinition(boolean dumpWithSchemaName, boolean doSchemaDumpPKs, boolean dumpFKsInsideTable, Properties colTypeConversionProp, Set<FK> foreignKeys) {
		List<String> pkCols = new ArrayList<String>();
		String tableName = (dumpWithSchemaName?schemaName+".":"")+name;
		
		StringBuffer sb = new StringBuffer();
		//Table
		sb.append("--drop table "+tableName+";\n");
		sb.append("create ");
		sb.append(getTableType4sql());
		sb.append("table "+tableName+" ( -- type="+type+"\n");
		//Columns
		for(Column c: columns) {
			String colDesc = Column.getColumnDesc(c, colTypeConversionProp, colTypeConversionProp.getProperty(SQLDump.PROP_FROM_DB_ID), colTypeConversionProp.getProperty(SQLDump.PROP_TO_DB_ID));
			if(c.pk) { pkCols.add(c.name); }
			sb.append("\t"+colDesc+",\n");
		}
		//PKs
		if(doSchemaDumpPKs && pkCols.size()>0) {
			sb.append("\tconstraint "+pkConstraintName+" primary key ("+Utils.join(pkCols, ", ")+"),\n");
		}
		//FKs?
		if(dumpFKsInsideTable) {
			sb.append(dumpFKsInsideTable(foreignKeys, schemaName, name, dumpWithSchemaName));
		}
		//Table end
		sb.delete(sb.length()-2, sb.length());
		//sb.append("\n);\n");
		sb.append("\n)");
		sb.append(getTableFooter4sql());
		sb.append(";\n");
		return sb.toString();
	}

	String dumpFKsInsideTable(Collection<FK> foreignKeys, String schemaName, String tableName, boolean dumpWithSchemaName) {
		StringBuffer sb = new StringBuffer();
		for(FK fk: foreignKeys) {
			if(schemaName.equals(fk.fkTableSchemaName) && tableName.equals(fk.fkTable)) {
				//sb.append("\tconstraint "+fk.getName()+" foreign key ("+Utils.join(fk.fkColumns, ", ")
				//	+") references "+(dumpWithSchemaName?fk.pkTableSchemaName+".":"")+fk.pkTable+" ("+Utils.join(fk.pkColumns, ", ")+"),\n");
				sb.append("\t"+FK.fkSimpleScript(fk, " ", dumpWithSchemaName)+",\n");
			}
		}
		return sb.toString();
	}
	
	public String getTableType4sql() {
		return "";
	}

	public String getTableFooter4sql() {
		return "";
	}
	
	//---------
	
	public TableType getType() {
		return type;
	}

	public void setType(TableType type) {
		this.type = type;
	}

	public List<Column> getColumns() {
		return columns;
	}

	public void setColumns(List<Column> columns) {
		this.columns = columns;
	}

	public List<Grant> getGrants() {
		return grants;
	}

	public void setGrants(List<Grant> grants) {
		this.grants = grants;
	}

	public String getPkConstraintName() {
		return pkConstraintName;
	}

	public void setPkConstraintName(String pkConstraintName) {
		this.pkConstraintName = pkConstraintName;
	}
	
}
