package tbrugz.sqldump.dbmodel;

import java.util.ArrayList;
import java.util.List;

import tbrugz.sqldump.util.SQLIdentifierDecorator;

/* implements Comparable<ExecutableObject>: not allowed?!
 * 
 * create package: http://www.stanford.edu/dept/itss/docs/oracle/10g/server.101/b10759/statements_6006.htm
 */
public class ExecutableObject extends DBObject {
	private static final long serialVersionUID = 1L;

	static transient SQLIdentifierDecorator sqlId = new SQLIdentifierDecorator();
	
	//public String type;
	DBObjectType type;
	String body;
	public final List<Grant> grants = new ArrayList<Grant>(); //XXX: should be Set<Grant>?
	String remarks;

	String packageName;
	List<ExecutableParameter> params;
	ExecutableParameter returnParam;
	
	@Override
	public String getDefinition(boolean dumpSchemaName) {
		//return "create "+type+" "+(dumpSchemaName?schemaName+".":"")+name+" as\n"+body;
		return (dumpCreateOrReplace?"create or replace ":"create ") 
				+ (body!=null ? body : 
					(
						getType()+" "+
						getFinalQualifiedName()+" /* has no body? */")
					);
	}
	
	@Override
	public String toString() {
		return "[Executable:"+type+":"+getSchemaName()+"."+getName()+
				(packageName!=null?";pkg="+packageName:"")
				+"]";
	}
	
	@Override
	public boolean isDumpable() {
		return body!=null;
	}
	
	@Override
	public String getFinalQualifiedName() {
		return (getSchemaName()!=null?sqlId.get(getSchemaName())+".":"")+
				(packageName!=null?sqlId.get(packageName)+".":"")+
				sqlId.get(getName());
	}

	@Override
	public String getQualifiedName() {
		return (getSchemaName()!=null?getSchemaName()+".":"")+
				(packageName!=null?packageName+".":"")+
				getName();
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

	public DBObjectType getType() {
		return type;
	}

	public void setType(DBObjectType type) {
		this.type = type;
	}
	
	public String getRemarks() {
		return remarks;
	}

	public void setRemarks(String remarks) {
		this.remarks = remarks;
	}

	public String getPackageName() {
		return packageName;
	}

	public void setPackageName(String packageName) {
		this.packageName = packageName;
	}

	public List<ExecutableParameter> getParams() {
		return params;
	}

	public void setParams(List<ExecutableParameter> params) {
		this.params = params;
	}

	public ExecutableParameter getReturnParam() {
		return returnParam;
	}

	public void setReturnParam(ExecutableParameter returnParam) {
		this.returnParam = returnParam;
	}

	public String getBody() {
		return body;
	}

	public void setBody(String body) {
		this.body = body;
	}
	
}
