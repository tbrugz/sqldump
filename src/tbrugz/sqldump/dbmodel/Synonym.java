package tbrugz.sqldump.dbmodel;

public class Synonym extends DBObject {
	boolean publik;
	public String objectOwner;
	public String referencedObject;
	public String dbLink;
	
	@Override
	public String getDefinition(boolean dumpSchemaName) {
		return "create "+(publik?"public ":"")+"synonym "+(dumpSchemaName?schemaName+".":"")+name
			+" for "+objectOwner+"."+referencedObject
			+(dbLink!=null?"@"+dbLink:"");
	}
}
