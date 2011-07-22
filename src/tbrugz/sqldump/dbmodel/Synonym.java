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
	
	@Override
	public String toString() {
		return "[Synonym:"+schemaName+"."+name+"->"+objectOwner+"."+referencedObject+"]";
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Synonym other = (Synonym) obj;
		if (dbLink == null) {
			if (other.dbLink != null)
				return false;
		} else if (!dbLink.equals(other.dbLink))
			return false;
		if (objectOwner == null) {
			if (other.objectOwner != null)
				return false;
		} else if (!objectOwner.equals(other.objectOwner))
			return false;
		if (publik != other.publik)
			return false;
		if (referencedObject == null) {
			if (other.referencedObject != null)
				return false;
		} else if (!referencedObject.equals(other.referencedObject))
			return false;
		return true;
	}
	
}
