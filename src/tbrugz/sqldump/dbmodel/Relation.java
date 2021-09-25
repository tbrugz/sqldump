package tbrugz.sqldump.dbmodel;

import java.util.List;

public interface Relation extends NamedDBObject, TypedDBObject, RemarkableDBObject {
	
	/*
	 * redeclaring NamedDBObject' getters so that introspection may work
	 * 
	 * see:
	 * - http://stackoverflow.com/questions/185004/java-beans-introspector-getbeaninfo-does-not-pickup-any-superinterfaces-propert
	 * - http://bugs.java.com/bugdatabase/view_bug.do?bug_id=4275879
	 */
	@Override
	public String getName();
	
	@Override
	public String getSchemaName();
	
	public void setName(String name);

	public void setSchemaName(String schemaName);
	
	/*public List<Column> getColumns();

	public void setColumns(List<Column> columns);*/
	
	public List<String> getColumnNames();

	public List<String> getColumnTypes();

	public List<String> getColumnRemarks();
	
	public int getColumnCount();

	public List<Constraint> getConstraints();
	
	@Override
	public String getRemarks();
	
	@Override
	public void setRemarks(String remarks);
	
	public String getRelationType();
	
	public List<Grant> getGrants();

	public void setGrants(List<Grant> grants);
	
	public Integer getParameterCount();

	public List<String> getParameterTypes();

	public List<String> getNamedParameterNames();
	
	public String getRemarksSnippet(boolean dumpSchemaName);
	
	public String getQualifiedName();
	
	//public String getFinalQualifiedName();
	
}
