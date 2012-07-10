package tbrugz.sqldump.dbmodel;

import java.util.Collection;

import javax.xml.bind.annotation.XmlTransient;

public abstract class DBIdentifiable {
	String schemaName;

	@XmlTransient
	public String name;
	//DBObjectType objtype;

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

	public static <T extends DBIdentifiable> T getDBIdentifiableByTypeSchemaAndName(Collection<? extends DBIdentifiable> dbids, DBObjectType type, String schemaName, String name) {
		for(DBIdentifiable d: dbids) {
			if(type.equals(getType4Diff(d)) 
					&& (d.getSchemaName()!=null?d.getSchemaName().equals(schemaName):true) 
					//XXX: better? //&& (schemaName!=null?d.getSchemaName().equals(schemaName):true) 
					&& d.getName().equals(name)) return (T) d;
		}
		return null;
	}

	public static <T extends DBIdentifiable> T getDBIdentifiableByTypeAndName(Collection<? extends DBIdentifiable> dbids, DBObjectType type, String name) {
		for(DBIdentifiable d: dbids) {
			if(type.equals(getType4Diff(d)) 
					&& d.getName().equals(name)) return (T) d;
		}
		return null;
	}
	
	public static <T extends DBIdentifiable> T getDBIdentifiableByTypeAndNameIgnoreCase(Collection<? extends DBIdentifiable> dbids, DBObjectType type, String name) {
		for(DBIdentifiable d: dbids) {
			if(type.equals(getType4Diff(d)) 
					&& d.getName().equalsIgnoreCase(name)) return (T) d;
		}
		return null;
	}
	
	public static DBObjectType getType(DBIdentifiable ident) {
		if(ident instanceof Column) { return DBObjectType.COLUMN; }
		if(ident instanceof Constraint) { return DBObjectType.CONSTRAINT; }
		if(ident instanceof ExecutableObject) { return DBObjectType.EXECUTABLE; }
		if(ident instanceof FK) { return DBObjectType.FK; }
		//Grant?
		if(ident instanceof Index) { return DBObjectType.INDEX; }
		if(ident instanceof Sequence) { return DBObjectType.SEQUENCE; }
		if(ident instanceof Synonym) { return DBObjectType.SYNONYM; }
		if(ident instanceof Table) { return DBObjectType.TABLE; }
		if(ident instanceof Trigger) { return DBObjectType.TRIGGER; }
		if(ident instanceof View) { return DBObjectType.VIEW; }
		throw new RuntimeException("getType: DBObjectType not defined for: "+ident.getClass().getName());
	}

	//used for 'DROP' statements
	public static DBObjectType getType4Diff(DBIdentifiable ident) {
		if(ident instanceof FK) { return DBObjectType.CONSTRAINT; }
		if(ident instanceof ExecutableObject) { return ((ExecutableObject)ident).type; }
		return DBIdentifiable.getType(ident);
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
	
}
