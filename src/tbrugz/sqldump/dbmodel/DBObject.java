package tbrugz.sqldump.dbmodel;

public abstract class DBObject implements Comparable<DBObject> {
	public String schemaName;
	public String name;

	public String getSchemaName() {
		return schemaName;
	}

	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	//XXX: sql dialect param?
	public abstract String getDefinition(boolean dumpSchemaName);
	
	public int compareTo(DBObject o) {
		return name.compareTo(o.name);
	}
	
}
