package tbrugz.sqldump.dbmodel;

import java.util.ArrayList;
import java.util.List;

import tbrugz.sqldump.util.Utils;

/*
 * TODO: comments on view and columns
 * 
 * XXX: option to always dump column names?
 * XXX: option to dump FKs inside view?
 * 
 * XXXxx: check option: LOCAL, CASCADED, NONE
 * see: http://publib.boulder.ibm.com/infocenter/iseries/v5r3/index.jsp?topic=%2Fsqlp%2Frbafywcohdg.htm
 */
public class View extends DBObject implements Relation {
	
	public enum CheckOptionType {
		LOCAL, CASCADED, NONE, 
		TRUE; //true: set for databases that doesn't have local and cascaded options 
	}
	
	public String query;
	
	public CheckOptionType checkOption;
	public boolean withReadOnly;
	List<String> columnNames = new ArrayList<String>();
	List<Constraint> constraints = new ArrayList<Constraint>();
	String remarks;
	List<String> columnRemarks = new ArrayList<String>();
	
	//public String checkOptionConstraintName;
	
	@Override
	public String getDefinition(boolean dumpSchemaName) {
		StringBuffer sb = new StringBuffer();
		for(Constraint cons: constraints) {
			sb.append(",\n\t"+cons.getDefinition(false));
		}

		StringBuffer sbRemarks = new StringBuffer();
		if(remarks!=null) {
			sbRemarks.append("\n\ncomment on table "+schemaName+"."+name+" is '"+remarks+"'");
		}
		if(columnRemarks!=null && columnRemarks.size()>0) {
			if(sbRemarks.length()>0) { sbRemarks.append(";"); }
			sbRemarks.append("\n");
			for(int i=0;i<columnRemarks.size();i++) {
				sbRemarks.append((i==0?"":";")+"\ncomment on column "+schemaName+"."+name+"."+columnNames.get(i)+" is '"+columnRemarks.get(i)+"'");
			}
		}
		
		return (dumpCreateOrReplace?"create or replace ":"create ") + "view "
				+ (dumpSchemaName && schemaName!=null?DBObject.getFinalIdentifier(schemaName)+".":"") + DBObject.getFinalIdentifier(name)
				+ (constraints.size()>0?" (\n\t"+Utils.join(columnNames, ", ")+sb.toString()+"\n)":"")
				+ " as\n" + query
				+ (withReadOnly?"\nwith read only":
					(checkOption!=null && !checkOption.equals(CheckOptionType.NONE)?
						"\nwith "+(checkOption.equals(CheckOptionType.TRUE)?"":checkOption+" ")+"check option":""
					)
				  )
				+ (sbRemarks.length()>0?";"+sbRemarks.toString():"");
	}
	
	@Override
	public String toString() {
		return "View["+name+"]";
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof View) {
			View v = (View) obj;
			return name.equals(v.name) && query.equals(v.query);
		}
		return false;
	}

	@Override
	public List<Constraint> getConstraints() {
		return constraints;
	}

	@Override
	public List<String> getColumnNames() {
		return columnNames;
	}

	@Override
	public String getRemarks() {
		return remarks;
	}

	public void setRemarks(String remarks) {
		this.remarks = remarks;
	}

	/*@Override
	public String getAfterCreateScript() {
		return null;
	}*/
}
