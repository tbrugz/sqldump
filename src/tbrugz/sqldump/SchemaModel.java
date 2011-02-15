package tbrugz.sqldump;

import java.util.HashSet;
import java.util.Set;

public class SchemaModel {
	Set<Table> tables = new HashSet<Table>();
	Set<FK> foreignKeys = new HashSet<FK>();
	Set<View> views = new HashSet<View>();
	Set<Trigger> triggers = new HashSet<Trigger>();
	Set<ExecutableObject> executables = new HashSet<ExecutableObject>();

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
	public Set<View> getViews() {
		return views;
	}
	public void setViews(Set<View> views) {
		this.views = views;
	}
	public Set<Trigger> getTriggers() {
		return triggers;
	}
	public void setTriggers(Set<Trigger> triggers) {
		this.triggers = triggers;
	}
	public Set<ExecutableObject> getExecutables() {
		return executables;
	}
	public void setExecutables(Set<ExecutableObject> executables) {
		this.executables = executables;
	}
	
	
}
