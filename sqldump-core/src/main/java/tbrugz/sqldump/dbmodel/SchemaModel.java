package tbrugz.sqldump.dbmodel;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;
import jakarta.xml.bind.annotation.XmlType;

/*
see: http://blog.bdoughan.com/2011/06/using-jaxbs-xmlaccessortype-to.html
*/
@XmlRootElement
//@XmlAccessorType(XmlAccessType.NONE)
@XmlType(propOrder={"tables", "foreignKeys", "views", "triggers", "executables", "indexes", "sequences", "synonyms", "schemaMetadata", "sqlDialect", "modelId", "metadata"})
public class SchemaModel implements Serializable {
	private static final long serialVersionUID = 1L;
	
	String modelId;
	String sqlDialect;

	Set<Table> tables = new TreeSet<Table>();
	Set<FK> foreignKeys = new TreeSet<FK>();
	Set<View> views = new TreeSet<View>();
	Set<Trigger> triggers = new TreeSet<Trigger>();
	Set<ExecutableObject> executables = new TreeSet<ExecutableObject>();
	Set<Synonym> synonyms = new TreeSet<Synonym>();
	Set<Index> indexes = new TreeSet<Index>();
	Set<Sequence> sequences = new TreeSet<Sequence>();
	Set<SchemaMetaData> schemasMetadata = new TreeSet<SchemaMetaData>();

	/*
	 * metadata should contain things like:
	 * - grabbed info timestamp
	 * - database url, database username
	 * - version of sqldump used
	 * see: tbrugz.sqldump.util.ModelMetaData
	 */
	Map<String,String> metadata;
	
	//XXX: add List<String>(?) schemasGrabbed? may be used by Schema2GraphML
	//XXX: add Set<Grant> schemaGrants?
	
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

	@XmlElement(name="schemaMetadata")
	public Set<SchemaMetaData> getSchemaMetadata() {
		return schemasMetadata;
	}
	public void setSchemaMetadata(Set<SchemaMetaData> schemasMetadata) {
		this.schemasMetadata = schemasMetadata;
	}

	@XmlElement(name="sqlDialect")
	public String getSqlDialect() {
		return sqlDialect;
	}
	public void setSqlDialect(String sqlDialect) {
		this.sqlDialect = sqlDialect;
	}
	
	@XmlElement(name="modelId")
	public String getModelId() {
		return modelId;
	}
	
	public void setModelId(String modelId) {
		this.modelId = modelId;
	}

	@XmlElement(name="metadata")
	public Map<String,String> getMetadata() {
		return metadata;
	}
	public void setMetadata(Map<String,String> metadata) {
		this.metadata = metadata;
	}
	
}
