package tbrugz.sqldump.dbmodel;

public class BaseNamedDBObject implements NamedDBObject {

	final String schemaName, tableName;
	
	public BaseNamedDBObject(String schemaName, String tableName) {
		this.schemaName = schemaName;
		this.tableName = tableName;
	}
	
	@Override
	public String getName() {
		return tableName;
	}
	
	@Override
	public String getSchemaName() {
		return schemaName;
	}
	
	@Override
	public String toString() {
		return "NamedTable:"+(schemaName!=null?schemaName+".":"")+tableName;
	}
	
	public int compareTo(NamedDBObject o) {
		int comp = schemaName!=null?schemaName.compareTo(o.getSchemaName()):o.getSchemaName()!=null?1:0; //XXX: return -1? 1?
		if(comp!=0) return comp;
		return tableName.compareTo(o.getName());
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj==null) { return false; }
		if(! (obj instanceof NamedDBObject)) { return false; }
		return compareTo((NamedDBObject) obj)==0;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
		result = prime * result + ((schemaName == null) ? 0 : schemaName.hashCode());
		return result;
	}

}
