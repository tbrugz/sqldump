package tbrugz.sqldump.dbmodel;

import java.util.List;

public interface Relation {
	
	public String getName();

	public void setName(String name);

	public String getSchemaName();

	public void setSchemaName(String schemaName);
	
	/*public List<Column> getColumns();

	public void setColumns(List<Column> columns);*/
	
	public List<String> getColumnNames();

	public List<Constraint> getConstraints();
	
	public String getRemarks();

}
