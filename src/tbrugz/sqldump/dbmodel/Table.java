package tbrugz.sqldump.dbmodel;

import java.util.ArrayList;
import java.util.List;


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
