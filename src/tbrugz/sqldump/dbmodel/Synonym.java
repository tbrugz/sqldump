package tbrugz.sqldump.dbmodel;

public class Synonym extends DBObject {
	boolean publik;
	public String objectOwner;
	public String referencedObject;
	public String dbLink;
	
	@Override
	public String getDefinition(boolean dumpSchemaName) {
		return "CREATE "+(publik?"PUBLIC ":"")+"SYNONYM "+(dumpSchemaName?schemaName+".":"")+name
			+" FOR "+objectOwner+"."+referencedObject
			+(dbLink!=null?"@"+dbLink:"")+";";
	}
}
