package tbrugz.sqldump.dbmodel;

import java.io.Serializable;
import java.util.Collection;

public abstract class DBObject implements Comparable<DBObject>, Serializable {
	private static final long serialVersionUID = 1L;

	public static class DBObjectId implements Comparable<DBObjectId> {
		public String schemaName;
		public String name;
		
		@Override
		public boolean equals(Object obj) {
			if(obj instanceof DBObjectId) {
				DBObjectId oid = (DBObjectId) obj;
				return name.equals(oid.name) && schemaName.equals(oid.schemaName);
			}
			return false;
		}

		public int compareTo(DBObjectId o) {
			int comp = schemaName.compareTo(o.schemaName);
			if(comp!=0) return comp;
			return name.compareTo(o.name);
		}
	}
	
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
	
	//XXX: getDefinition() should have 'sql dialect' param?
	public abstract String getDefinition(boolean dumpSchemaName);
	
	public int compareTo(DBObject o) {
		int comp = schemaName.compareTo(o.schemaName);
		if(comp!=0) return comp;
		return name.compareTo(o.name);
	}
	
	public static DBObject findDBObjectByName(Collection<? extends DBObject> col, String name) {
		for(DBObject obj: col) {
			if(name.equals(obj.name)) return obj;
		}
		return null;
	}
}
