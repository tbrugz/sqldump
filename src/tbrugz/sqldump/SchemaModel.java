package tbrugz.sqldump;

import java.io.Serializable;
import java.util.Set;
import java.util.TreeSet;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import tbrugz.sqldump.dbmodel.ExecutableObject;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.Index;
import tbrugz.sqldump.dbmodel.Sequence;
import tbrugz.sqldump.dbmodel.Synonym;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.dbmodel.Trigger;
import tbrugz.sqldump.dbmodel.View;

@XmlRootElement
@XmlType(propOrder={"tables", "foreignKeys", "views", "triggers", "executables", "indexes", "sequences", "synonyms"})
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

	@XmlElement(name="table")
	public Set<Table> getTables() {
		return tables;
	}
	public void setTables(Set<Table> tables) {
		this.tables = tables;
	}
	
	@XmlElement(name="foreignKey")
	public Set<FK> getForeignKeys() {
		return foreignKeys;
	}
	public void setForeignKeys(Set<FK> foreignKeys) {
		this.foreignKeys = foreignKeys;
	}
	
	@XmlElement(name="view")
	public Set<View> getViews() {
		return views;
	}
	public void setViews(Set<View> views) {
		this.views = views;
	}
	
	@XmlElement(name="trigger")
	public Set<Trigger> getTriggers() {
		return triggers;
	}
	public void setTriggers(Set<Trigger> triggers) {
		this.triggers = triggers;
	}
	
	@XmlElement(name="executable")
	public Set<ExecutableObject> getExecutables() {
		return executables;
	}
	public void setExecutables(Set<ExecutableObject> executables) {
		this.executables = executables;
	}
	
	@XmlElement(name="synonym")
	public Set<Synonym> getSynonyms() {
		return synonyms;
	}
	public void setSynonyms(Set<Synonym> synonyms) {
		this.synonyms = synonyms;
	}
	
	@XmlElement(name="index")
	public Set<Index> getIndexes() {
		return indexes;
	}
	public void setIndexes(Set<Index> indexes) {
		this.indexes = indexes;
	}
	
	@XmlElement(name="sequence")
	public Set<Sequence> getSequences() {
		return sequences;
	}
	public void setSequences(Set<Sequence> sequences) {
		this.sequences = sequences;
	}
	
}
