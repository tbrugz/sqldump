package tbrugz.sqldump.dbmodel;

import java.io.Serializable;
import java.util.Collection;

import tbrugz.sqldump.util.SQLIdentifierDecorator;
import tbrugz.sqldump.util.StringDecorator;

public abstract class DBObject extends DBIdentifiable implements Comparable<DBObject>, Serializable {
	private static final long serialVersionUID = 1L;
	
	//dumping parameters
	//XXX: add dumpWithSchemaName to DBObject ?
	public static transient boolean dumpCreateOrReplace = false;
	public static transient SQLIdentifierDecorator sqlIddecorator = new SQLIdentifierDecorator();
	//public static transient boolean dumpQuoteAll = true;
	//public static transient String dumpIdentifierQuoteString = "\"";

	//XXX: getDefinition() should have 'sql dialect' param?
	public abstract String getDefinition(boolean dumpSchemaName);
	
	public int compareTo(DBObject o) {
		int comp = schemaName!=null?schemaName.compareTo(o.schemaName):o.schemaName!=null?1:0; //XXX: return -1? 1?
		if(comp!=0) return comp;
		return name.compareTo(o.name);
	}
	
	public String getQualifiedName() {
		return (schemaName!=null?schemaName+".":"")+name;
	}

	public String getFinalQualifiedName() {
		return getFinalName(true);
	}
	
	public String getFinalName(boolean dumpSchemaName) {
		return ((dumpSchemaName && schemaName!=null)?
				sqlIddecorator.get(schemaName)+".":"")+sqlIddecorator.get(name);
	}
	
	//XXX: move to DBIdentifiable?
	public static DBObject findDBObjectBySchemaAndName(Collection<? extends DBObject> col, String schemaName, String name) {
		for(DBObject obj: col) {
			if(schemaName.equals(obj.schemaName) && name.equals(obj.name)) return obj;
		}
		return null;
	}
	
	public static String getFinalIdentifier(String id) {
		return sqlIddecorator.get(id);
	}
	
	public static String getFinalName(NamedDBObject dbobject, boolean dumpSchemaName) {
		return ((dumpSchemaName && dbobject.getSchemaName()!=null)?
				sqlIddecorator.get(dbobject.getSchemaName())+".":"")+sqlIddecorator.get(dbobject.getName());
	}

	public static String getFinalName(NamedDBObject dbobject, StringDecorator decorator, boolean dumpSchemaName) {
		return ((dumpSchemaName && dbobject.getSchemaName()!=null)?
				decorator.get(dbobject.getSchemaName())+".":"")+decorator.get(dbobject.getName());
	}
	
	public static String getFinalName(String schemaName, String name, boolean dumpSchemaName) {
		return ((dumpSchemaName && schemaName!=null)?
				sqlIddecorator.get(schemaName)+".":"")+sqlIddecorator.get(name);
	}
}
