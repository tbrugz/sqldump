package tbrugz.sqldump.dbmodel;

import java.util.Collection;

public abstract class DBIdentifiable implements NamedDBObject, Comparable<DBIdentifiable> {
	String schemaName;
	String name;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getSchemaName() {
		return schemaName;
	}

	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}
	
	//XXX: getDefinition() should have 'sql dialect' param?
	public abstract String getDefinition(boolean dumpSchemaName);
	
	public boolean isDumpable() {
		return true;
	}

	@SuppressWarnings("unchecked")
	public static <T extends DBIdentifiable> T getDBIdentifiableByTypeSchemaAndName(Collection<? extends DBIdentifiable> dbids, DBObjectType type, String schemaName, String name) {
		for(DBIdentifiable d: dbids) {
			if(type.equals(getType(d)) 
					&& (d.getSchemaName()!=null?d.getSchemaName().equals(schemaName):true) 
					&& d.getName().equals(name)) { return (T) d; }
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	public static <T extends DBIdentifiable> T getDBIdentifiableByTypeSchemaAndNameIgnoreCase(Collection<? extends DBIdentifiable> dbids, DBObjectType type, String schemaName, String name) {
		for(DBIdentifiable d: dbids) {
			if(type.equals(getType(d)) 
					&& (d.getSchemaName()!=null?d.getSchemaName().equalsIgnoreCase(schemaName):true) 
					&& d.getName().equalsIgnoreCase(name)) { return (T) d; }
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	public static <T extends DBIdentifiable> T getDBIdentifiableByTypeAndName(Collection<? extends DBIdentifiable> dbids, DBObjectType type, String name) {
		for(DBIdentifiable d: dbids) {
			if(type.equals(getType(d)) 
					&& d.getName().equals(name)) { return (T) d; }
		}
		return null;
	}
	
	@SuppressWarnings("unchecked")
	public static <T extends DBIdentifiable> T getDBIdentifiableByTypeAndNameIgnoreCase(Collection<? extends DBIdentifiable> dbids, DBObjectType type, String name) {
		for(DBIdentifiable d: dbids) {
			if(type.equals(getType(d)) 
					&& d.getName().equalsIgnoreCase(name)) { return (T) d; }
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	public static <T extends DBIdentifiable> T getDBIdentifiableBySchemaAndName(Collection<? extends DBIdentifiable> dbids, String schemaName, String name) {
		for(DBIdentifiable obj: dbids) {
			if(obj!=null
					&& ( (schemaName==null && obj.schemaName==null) || (schemaName!=null && schemaName.equals(obj.schemaName)) ) 
					&& name.equals(obj.name) ) { return (T) obj; }
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	public static <T extends DBIdentifiable> T getDBIdentifiableBySchemaAndNameIgnoreCase(Collection<? extends DBIdentifiable> dbids, String schemaName, String name) {
		for(DBIdentifiable obj: dbids) {
			if(obj!=null
					&& ( (schemaName==null && obj.schemaName==null) || (schemaName!=null && schemaName.equalsIgnoreCase(obj.schemaName)) ) 
					&& name.equalsIgnoreCase(obj.name) ) { return (T) obj; }
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	public static <T extends DBIdentifiable> T getDBIdentifiableByName(Collection<? extends DBIdentifiable> dbids, String name) {
		for(DBIdentifiable d: dbids) {
			if(d.getName()!=null && d.getName().equals(name)) { return (T) d; }
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	public static <T extends DBIdentifiable> T getDBIdentifiableByNamedObject(Collection<? extends DBIdentifiable> dbids, NamedDBObject object) {
		for(DBIdentifiable obj: dbids) {
			if(( (object.getSchemaName()==null && obj.schemaName==null) || object.getSchemaName().equals(obj.schemaName)) 
					&& object.getName().equals(obj.name)) { return (T) obj; }
		}
		return null;
	}
	
	public static DBObjectType getType(DBIdentifiable ident) {
		if(ident instanceof Column) { return DBObjectType.COLUMN; }
		if(ident instanceof Constraint) { return DBObjectType.CONSTRAINT; }
		//if(ident instanceof ExecutableObject) { return DBObjectType.EXECUTABLE; }
		if(ident instanceof ExecutableObject) { return ((ExecutableObject)ident).type; }
		if(ident instanceof FK) { return DBObjectType.FK; }
		//Grant?
		if(ident instanceof Index) { return DBObjectType.INDEX; }
		if(ident instanceof Sequence) { return DBObjectType.SEQUENCE; }
		if(ident instanceof Synonym) { return DBObjectType.SYNONYM; }
		if(ident instanceof Table) { return DBObjectType.TABLE; }
		if(ident instanceof Trigger) { return DBObjectType.TRIGGER; }
		if(ident instanceof MaterializedView) { return DBObjectType.MATERIALIZED_VIEW; } //MaterializedView extends View: must come first in "if" order
		if(ident instanceof View) { return DBObjectType.VIEW; }
		
		throw new RuntimeException("getType: DBObjectType not defined for: "+ident.getClass().getName());
	}

	//used for 'ALTER TABLE <xxx> DROP' statements
	public static DBObjectType getType4Alter(DBIdentifiable ident) {
		if(ident instanceof FK) { return DBObjectType.CONSTRAINT; }
		//if(ident instanceof MaterializedView) { return DBObjectType.MATERIALIZED_VIEW; }
		//if(ident instanceof ExecutableObject) { return ((ExecutableObject)ident).type; }
		return DBIdentifiable.getType(ident);
	}

	//used for 'DROP' statements
	public static DBObjectType getType4Alter(DBObjectType type) {
		if(type.equals(DBObjectType.FK)) { return DBObjectType.CONSTRAINT; }
		return type;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result
				+ ((schemaName == null) ? 0 : schemaName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DBIdentifiable other = (DBIdentifiable) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (schemaName == null) {
			if (other.schemaName != null)
				return false;
		} else if (!schemaName.equals(other.schemaName))
			return false;
		return true;
	}
	
	public boolean equals4Diff(DBIdentifiable obj) {
		return equals(obj);
	}
	
	//XXXdone: use schemaName in compareTo()? see old DBObject.compareTo()...
	@Override
	public int compareTo(DBIdentifiable o) {
		int comp = getType(this).compareTo(getType(o));
		if(comp==0) {
			if(getSchemaName()!=null) {
				if(o.getSchemaName()!=null) {
					comp = getSchemaName().compareTo(o.getSchemaName());
				}
				else {
					comp = 1;
				}
			}
			else {
				if(o.getSchemaName()!=null) {
					comp = -1;
				}
				//else { comp = 0; }
			}
		}
		if(comp==0) {
			comp = getName().compareTo(o.getName());
		}
		return comp;
	}
	
}
