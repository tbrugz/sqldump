package tbrugz.sqldump.dbmodel;

public class Trigger extends DBObject {
	private static final long serialVersionUID = 1L;

	//TODO: add transient String name, add get/set
	public String description;
	public String body;
	public String tableName;
	
	@Override
	public String getDefinition(boolean dumpSchemaName) {
		return (dumpCreateOrReplace?"create or replace ":"create ") + "trigger "
				+ description + "\n" + body;
	}

	@Override
	public String toString() {
		return "[Trigger:"+description+"]";
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Trigger other = (Trigger) obj;
		if (body == null) {
			if (other.body != null)
				return false;
		} else if (!body.equals(other.body))
			return false;
		if (description == null) {
			if (other.description != null)
				return false;
		} else if (!description.equals(other.description))
			return false;
		if (tableName == null) {
			if (other.tableName != null)
				return false;
		} else if (!tableName.equals(other.tableName))
			return false;
		return true;
	}
	
}
