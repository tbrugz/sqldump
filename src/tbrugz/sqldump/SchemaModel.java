package tbrugz.sqldump;

import java.util.HashSet;
import java.util.Set;

public class SchemaModel {
	Set<Table> tables = new HashSet<Table>();
	Set<FK> foreignKeys = new HashSet<FK>();

	public Set<Table> getTables() {
		return tables;
	}
	public void setTables(Set<Table> tables) {
		this.tables = tables;
	}
	public Set<FK> getForeignKeys() {
		return foreignKeys;
	}
	public void setForeignKeys(Set<FK> foreignKeys) {
		this.foreignKeys = foreignKeys;
	}
	
}
