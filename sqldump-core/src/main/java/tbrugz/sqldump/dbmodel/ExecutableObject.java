package tbrugz.sqldump.dbmodel;

import java.util.ArrayList;
import java.util.List;

import tbrugz.sqldiff.WhitespaceIgnoreType;
import tbrugz.sqldump.util.SQLIdentifierDecorator;
import tbrugz.sqldump.util.StringUtils;

/* implements Comparable<ExecutableObject>: not allowed?!
 * 
 * create package: http://www.stanford.edu/dept/itss/docs/oracle/10g/server.101/b10759/statements_6006.htm
 * 
 * see: https://en.wikipedia.org/wiki/SQL/JRT
 */
public class ExecutableObject extends DBObject implements TypedDBObject, ParametrizedDBObject, RemarkableDBObject, BodiedObject {

	private static final long serialVersionUID = 1L;

	static transient SQLIdentifierDecorator sqlId = new SQLIdentifierDecorator();
	
	//public String type;
	DBObjectType type;
	String body;
	final List<Grant> grants = new ArrayList<Grant>(); //XXX: should be Set<Grant>?
	String remarks;
	boolean deterministic;

	String packageName;
	protected List<ExecutableParameter> params;
	protected ExecutableParameter returnParam;
	
	@Override
	public String getDefinition(boolean dumpSchemaName) {
		//return "create "+type+" "+(dumpSchemaName?schemaName+".":"")+name+" as\n"+body;
		String preamble = "";
		if(!type.equals(DBObjectType.JAVA_SOURCE)) {
			preamble = (dumpCreateOrReplace?"create or replace ":"create ");
		}
		return preamble
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
	public int compareTo(DBIdentifiable o) {
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
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((body == null) ? 0 : body.hashCode());
		result = prime * result
				+ ((packageName == null) ? 0 : packageName.hashCode());
		result = prime * result + ((params == null) ? 0 : params.hashCode());
		result = prime * result
				+ ((returnParam == null) ? 0 : returnParam.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
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
		ExecutableObject other = (ExecutableObject) obj;
		if (body == null) {
			if (other.body != null)
				return false;
		} else if (!body.equals(other.body))
			return false;
		if (packageName == null) {
			if (other.packageName != null)
				return false;
		} else if (!packageName.equals(other.packageName))
			return false;
		if (params == null) {
			if (other.params != null)
				return false;
		} else if (!params.equals(other.params))
			return false;
		if (returnParam == null) {
			if (other.returnParam != null)
				return false;
		} else if (!returnParam.equals(other.returnParam))
			return false;
		if (type != other.type)
			return false;
		return true;
	}

	/* ignoring whitespaces */
	@Override
	public boolean equals4Diff(DBIdentifiable obj, WhitespaceIgnoreType wsIgnore) {
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
		} else if (!StringUtils.equalsIgnoreWhitespacesEachLine(body, other.body, wsIgnore))
		//} else if (!body.equals(other.body))
			return false;
		if (packageName == null) {
			if (other.packageName != null)
				return false;
		} else if (!packageName.equals(other.packageName))
			return false;
		if (params == null) {
			if (other.params != null)
				return false;
		} else if (!params.equals(other.params))
			return false;
		if (returnParam == null) {
			if (other.returnParam != null)
				return false;
		} else if (!returnParam.equals(other.returnParam))
			return false;
		if (type != other.type)
			return false;
		return true;
	}
	
	@Override
	public DBObjectType getDbObjectType() {
		return type;
	}
	
	@Override
	public DBObjectType getDBObjectType() {
		return getDbObjectType();
	}
	
	public DBObjectType getType() {
		return type;
	}
	
	public void setType(DBObjectType type) {
		this.type = type;
	}
	
	@Override
	public String getRemarks() {
		return remarks;
	}

	@Override
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

	@Override
	public String getBody() {
		return body;
	}

	@Override
	public void setBody(String body) {
		this.body = body;
	}

	public List<Grant> getGrants() {
		return grants;
	}

	@Override
	public Integer getParameterCount() {
		if(params==null) { return null; }
		return params.size();
	}
	
	@Override
	public List<String> getParameterTypes() {
		if(params==null) { return null; }
		List<String> types = new ArrayList<String>();
		for(ExecutableParameter ep: params) {
			types.add(ep.dataType);
		}
		return types;
	}
	
	public boolean isDeterministic() {
		return deterministic;
	}

	public void setDeterministic(boolean deterministic) {
		this.deterministic = deterministic;
	}

}
