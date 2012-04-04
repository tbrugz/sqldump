package tbrugz.sqldump.dbmodel;

import java.util.ArrayList;
import java.util.List;

/* implements Comparable<ExecutableObject>: not allowed?!
 * 
 * create package: http://www.stanford.edu/dept/itss/docs/oracle/10g/server.101/b10759/statements_6006.htm
 */
public class ExecutableObject extends DBObject {

	//public String type;
	public DBObjectType type;
	public String body;
	public List<Grant> grants = new ArrayList<Grant>(); //XXX: should be Set<Grant>?
	
	@Override
	public String getDefinition(boolean dumpSchemaName) {
		//return "create "+type+" "+(dumpSchemaName?schemaName+".":"")+name+" as\n"+body;
		return (dumpCreateOrReplace?"create or replace ":"create ") + body;
	}
	
	@Override
	public String toString() {
		return "[Executable:"+type+":"+schemaName+"."+name+"]";
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

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		ExecutableObject other = (ExecutableObject) obj;
		if (body == null) {
			if (other.body != null)
				return false;
		} else if (!body.equals(other.body))
			return false;
		if (type != other.type)
			return false;
		return true;
	}
	
}
