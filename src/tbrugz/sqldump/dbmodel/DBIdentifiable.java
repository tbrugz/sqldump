package tbrugz.sqldump.dbmodel;

import java.util.Collection;

public abstract class DBIdentifiable {
	String name;
	//DBObjectType objtype;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	//XXX: getDefinition() should have 'sql dialect' param?
	public abstract String getDefinition(boolean dumpSchemaName);

	public static <T extends DBIdentifiable> T getDBIdentifiableByName(Collection<? extends DBIdentifiable> dbid, String colName) {
		for(DBIdentifiable d: dbid) {
			if(d.getName().equals(colName)) return (T) d;
		}
		return null;
	}
	
	public static DBObjectType getType(DBIdentifiable ident) {
		if(ident instanceof Column) { return DBObjectType.COLUMN; }
		if(ident instanceof Constraint) { return DBObjectType.CONSTRAINT; }
		if(ident instanceof FK) { return DBObjectType.FK; }
		throw new RuntimeException("DBObjectType not defined for: "+ident.getClass().getName());
	}
	
}
