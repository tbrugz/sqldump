package tbrugz.sqldump.dbmodel;

import java.util.ArrayList;
import java.util.List;

import tbrugz.sqldump.util.Utils;

/*
 * TODOne: comments on view and columns
 * 
 * XXX: option to always dump column names?
 * XXX: option to dump FKs inside view?
 * 
 * XXXxx: check option: LOCAL, CASCADED, NONE
 * see: http://publib.boulder.ibm.com/infocenter/iseries/v5r3/index.jsp?topic=%2Fsqlp%2Frbafywcohdg.htm
 */
public class View extends DBObject implements Relation {
	private static final long serialVersionUID = 1L;

	public enum CheckOptionType {
		LOCAL, CASCADED, NONE, 
		TRUE; //true: set for databases that doesn't have local and cascaded options 
	}
	
	public String query;
	
	public CheckOptionType checkOption;
	public boolean withReadOnly;
	List<Column> columns = null;
	List<Constraint> constraints = null;
	String remarks;
	
	//public String checkOptionConstraintName;
	
	@Override
	public String getDefinition(boolean dumpSchemaName) {
		StringBuffer sbConstraints = new StringBuffer();
		if(constraints!=null) {
			for(int i=0;i<constraints.size();i++) {
				Constraint cons = constraints.get(i);
				sbConstraints.append((i==0?"":",\n\t")+cons.getDefinition(false));
			}
		}

		StringBuffer sbRemarks = new StringBuffer();

		String stmp1 = Table.getRelationRemarks(this, dumpSchemaName);
		String stmp2 = Table.getColumnRemarks(columns, this, dumpSchemaName);
		
		if(stmp1!=null && stmp1.length()>0 && stmp2!=null && stmp2.length()>0) { sbRemarks.append("\n\n"); }
		if(stmp1!=null && stmp1.length()>0) { sbRemarks.append(stmp1+";\n"); }

		if(stmp2!=null && stmp2.length()>0) { sbRemarks.append(stmp2+";\n"); }
		int len = sbRemarks.length();
		if(len>0) {
			sbRemarks.delete(len-2, len);
		}
		
		return (dumpCreateOrReplace?"create or replace ":"create ") + "view "
				+ getFinalQualifiedName(dumpSchemaName)
				+ (sbConstraints.length()>0?" (\n\t"
						+ ((columns!=null&&columns.size()>0)?Utils.join(getColumnNames(), ", ")+",\n\t":"")
						+ sbConstraints.toString()+"\n)":"")
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
		return "View["+getName()+"]";
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
		return Table.getColumnNames(columns);
	}

	@Override
	public String getRemarks() {
		return remarks;
	}

	public void setRemarks(String remarks) {
		this.remarks = remarks;
	}

	public List<Column> getColumns() {
		return columns;
	}

	public void setColumns(List<Column> columns) {
		this.columns = columns;
	}
	
	public void setSimpleColumns(List<Column> columns) {
		if(columns==null) {
			this.columns = null;
			return;
		}
		
		this.columns = new ArrayList<Column>();
		for(int i=0;i<columns.size();i++) {
			Column tcol = columns.get(i);
			Column c = new Column();
			c.setName(tcol.getName());
			c.setRemarks(tcol.getRemarks());
			this.columns.add(c);
		}
	}

	public void setConstraints(List<Constraint> constraints) {
		this.constraints = constraints;
	}

	/*@Override
	public String getAfterCreateScript() {
		return null;
	}*/
}
