package tbrugz.sqldump.dbmodel;

public class Synonym extends DBObject {
	private static final long serialVersionUID = 1L;

	boolean publik;
	public String objectOwner;
	public String referencedObject;
	public String dbLink;
	
	@Override
	public String getDefinition(boolean dumpSchemaName) {
		//XXX: option to use "create or replace" for synonym?
		//return "create "
		return (dumpCreateOrReplace?"create or replace ":"create ") 
			+(publik?"public ":"")+"synonym "+getFinalName(dumpSchemaName)
			+" for "+DBObject.getFinalName(objectOwner, referencedObject, dumpSchemaName)
			+(dbLink!=null?"@"+dbLink:"");
	}
	
	@Override
	public String toString() {
		return "[Synonym:"+getSchemaName()+"."+getName()+"->"+objectOwner+"."+referencedObject+"]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((dbLink == null) ? 0 : dbLink.hashCode());
		result = prime * result
				+ ((objectOwner == null) ? 0 : objectOwner.hashCode());
		result = prime * result + (publik ? 1231 : 1237);
		result = prime
				* result
				+ ((referencedObject == null) ? 0 : referencedObject.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
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
