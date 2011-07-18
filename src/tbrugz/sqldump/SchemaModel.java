package tbrugz.sqldump;

import java.io.Serializable;
import java.util.Set;
import java.util.TreeSet;

import javax.xml.bind.annotation.XmlRootElement;

import tbrugz.sqldump.dbmodel.ExecutableObject;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.Index;
import tbrugz.sqldump.dbmodel.Sequence;
import tbrugz.sqldump.dbmodel.Synonym;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.dbmodel.Trigger;
import tbrugz.sqldump.dbmodel.View;

@XmlRootElement
public class SchemaModel implements Serializable {
	private static final long serialVersionUID = 1L;

	Set<Table> tables = new TreeSet<Table>();
	Set<FK> foreignKeys = new TreeSet<FK>();
	Set<View> views = new TreeSet<View>();
	Set<Trigger> triggers = new TreeSet<Trigger>();
	Set<ExecutableObject> executables = new TreeSet<ExecutableObject>();
	Set<Synonym> synonyms = new TreeSet<Synonym>();
	Set<Index> indexes = new TreeSet<Index>();
	Set<Sequence> sequences = new TreeSet<Sequence>();

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
	public Set<Synonym> getSynonyms() {
		return synonyms;
	}
	public void setSynonyms(Set<Synonym> synonyms) {
		this.synonyms = synonyms;
	}
	public Set<Index> getIndexes() {
		return indexes;
	}
	public void setIndexes(Set<Index> indexes) {
		this.indexes = indexes;
	}
	public Set<Sequence> getSequences() {
		return sequences;
	}
	public void setSequences(Set<Sequence> sequences) {
		this.sequences = sequences;
	}
	
}
