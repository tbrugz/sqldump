package tbrugz.sqldump.dbmodel;

// implements Comparable<ExecutableObject>: not allowed?!
public class ExecutableObject extends DBObject {
	public String type;
	public String body;
	
	@Override
	public String getDefinition(boolean dumpSchemaName) {
		//return "create "+type+" "+(dumpSchemaName?schemaName+".":"")+name+" as\n"+body;
		return "create "+body;
	}
	
	@Override
	public int compareTo(DBObject o) {
		if(o instanceof ExecutableObject) {
			ExecutableObject eo = (ExecutableObject) o;
    		//System.out.println("EO.compareTo: "+this+"/"+eo);
    		int typeCompare = type.compareTo(eo.type);
    		if(typeCompare==0) { //if same type, compare name (calling "super()")
    			return super.compareTo(eo);
    		}
    		return typeCompare;
		}
		return super.compareTo(o);
	}
}
