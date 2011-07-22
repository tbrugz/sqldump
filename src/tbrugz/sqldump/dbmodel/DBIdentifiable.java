package tbrugz.sqldump.dbmodel;

import java.util.Collection;

import javax.xml.bind.annotation.XmlTransient;

public abstract class DBIdentifiable {
	@XmlTransient
	public String schemaName;

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
					&& d.getName().equals(name)) return (T) d;
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
		return true;
	}
	
}
