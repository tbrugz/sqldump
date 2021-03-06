package tbrugz.sqldump.dbmodel;

import java.io.Serializable;

import tbrugz.sqldump.util.SQLIdentifierDecorator;
import tbrugz.sqldump.util.StringDecorator;

public abstract class DBObject extends DBIdentifiable implements Comparable<DBIdentifiable>, Serializable, ValidatableDBObject {
	
	private static final long serialVersionUID = 1L;
	
	Boolean valid;

	//dumping parameters
	//XXX: add dumpWithSchemaName to DBObject ?
	public static transient boolean dumpCreateOrReplace = false;
	public static final transient SQLIdentifierDecorator sqlIddecorator = new SQLIdentifierDecorator();
	//public static transient boolean dumpQuoteAll = true;
	//public static transient String dumpIdentifierQuoteString = "\"";

	//XXX: getDefinition() should have 'sql dialect' param?
	public abstract String getDefinition(boolean dumpSchemaName);
	
	/*
	@Override
	public int compareTo(DBIdentifiable di) {
		//return super.compareTo(di);
		if(!(di instanceof DBObject)) {
			return super.compareTo(di);
		}
		
		DBObject o = (DBObject) di;
		int comp = schemaName!=null?schemaName.compareTo(o.schemaName):o.schemaName!=null?1:0; //XXX: return -1? 1?
		if(comp!=0) return comp;
		return name.compareTo(o.name);
	}
	*/
	
	public String getQualifiedName() {
		return (schemaName!=null?schemaName+".":"")+name;
	}

	public String getFinalQualifiedName() {
		return getFinalName(true);
	}
	
	String getFinalName(boolean dumpSchemaName) {
		return ((dumpSchemaName && schemaName!=null)?
				sqlIddecorator.get(schemaName)+".":"")+sqlIddecorator.get(name);
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
	
	//public abstract String getRemarks();
	
	//public abstract void setRemarks(String remarks);
	
	@Override
	public Boolean getValid() {
		return valid;
	}

	@Override
	public void setValid(Boolean valid) {
		this.valid = valid;
	}

}
