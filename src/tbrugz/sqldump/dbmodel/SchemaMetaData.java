package tbrugz.sqldump.dbmodel;

public class SchemaMetaData extends DBObject {

	private static final long serialVersionUID = 1L;

	//XXX: add owner? charset?
	
	@Override
	public String getDefinition(boolean dumpSchemaName) {
		return "create schema "+name;
	}
	
	@Override
	public String toString() {
		return "SchemaMetaData[name="+name+"]";
	}

	public static SchemaMetaData newSchemaMetaData(String schemaName) {
		SchemaMetaData smd = new SchemaMetaData();
		//smd.schemaName = schemaName;
		smd.name = schemaName;
		return smd;
	}

}
