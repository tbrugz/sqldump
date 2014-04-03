package tbrugz.sqldump.dbmodel;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import tbrugz.sqldump.util.Utils;

/*
 * TODOne: comments on view and columns
 * 
 * XXX: option to always dump column names?
 * XXX: option to dump FKs inside view?
 * 
 * XXXxx: check option: LOCAL, CASCADED, NONE
 * see: http://publib.boulder.ibm.com/infocenter/iseries/v5r3/index.jsp?topic=%2Fsqlp%2Frbafywcohdg.htm
 * 
 * create view refs:
 * http://www.postgresql.org/docs/9.2/static/sql-createview.html
 * http://dev.mysql.com/doc/refman/5.0/en/create-view.html
 */
public class View extends DBObject implements Relation {
	private static final long serialVersionUID = 1L;

	public enum CheckOptionType {
		LOCAL, CASCADED, NONE, 
		TRUE; //true: set for databases that doesn't have local and cascaded options 
	}
	
	String query;
	
	CheckOptionType checkOption;
	boolean withReadOnly;
	List<Column> columns = null;
	List<Constraint> constraints = null;
	String remarks;
	// useful for Queries & parameterized views  
	// see: http://asktom.oracle.com/pls/asktom/f?p=100:11:0::::P11_QUESTION_ID:1448404423206 ,
	//  http://stackoverflow.com/questions/4498364/create-parameterized-view-in-sql-server-2008 ,
	//  http://hordine.com/2012/08/workaround-for-parameterized-views-in-mysql/
	Integer parameterCount; //XXXdone add parameterCount to View?
	
	//public String checkOptionConstraintName;
	
	static final Pattern PATTERN_CREATE_VIEW = Pattern.compile("\\s*create\\s+", Pattern.CASE_INSENSITIVE);
	//static final Pattern PATTERN_CREATE_VIEW = Pattern.compile("\\s*create\\s+(or\\s+replace\\s+)?(temp(orary)?\\s+)?(force\\s+)?view\\s+", Pattern.CASE_INSENSITIVE);
	
	@Override
	public String getDefinition(boolean dumpSchemaName) {
		return getDefinition(dumpSchemaName, null);
	}

	protected String getDefinition(boolean dumpSchemaName, String viewType) {
		if(PATTERN_CREATE_VIEW.matcher(query).find()) {
			return query;
				//+ (sbRemarks.length()>0?";"+sbRemarks.toString():"");
		}
		
		return (dumpCreateOrReplace?"create or replace ":"create ") + (viewType!=null?viewType+" ":"") + "view "
				+ getFinalName(dumpSchemaName)
				+ getConstraintsSnippet()
				+ getExtraConstraintsSnippet()
				+ " as\n" + query
				+ getCheckOptionAndReadOnlySnippet()
				+ getRemarksSnippet(dumpSchemaName);
	}
	
	protected String getConstraintsSnippet() {
		StringBuffer sbConstraints = new StringBuffer();
		if(constraints!=null) {
			for(int i=0;i<constraints.size();i++) {
				Constraint cons = constraints.get(i);
				sbConstraints.append((i==0?"":",\n\t")+cons.getDefinition(false));
			}
		}
		
		if(sbConstraints.length()>0) {
			return " (\n\t"
						+ ((columns!=null&&columns.size()>0)?Utils.join(getColumnNames(), ", ")+",\n\t":"")
						+ sbConstraints.toString()+"\n)";			
		}
		
		return "";
	}
	
	protected String getExtraConstraintsSnippet() {
		return "";
	}
	
	protected String getCheckOptionAndReadOnlySnippet() {
		return (withReadOnly?"\nwith read only":
					(checkOption!=null && !checkOption.equals(CheckOptionType.NONE)?
						"\nwith "+(checkOption.equals(CheckOptionType.TRUE)?"":checkOption+" ")+"check option":""
					)
				);		
	}
	
	protected String getRemarksSnippet(boolean dumpSchemaName) {
		StringBuffer sbRemarks = new StringBuffer();

		String stmp1 = Table.getRelationRemarks(this, dumpSchemaName);
		String stmp2 = Table.getColumnRemarks(columns, this, dumpSchemaName);
		
		if( (stmp1!=null && stmp1.length()>0) || (stmp2!=null && stmp2.length()>0) ) { sbRemarks.append("\n\n"); }
		
		if(stmp1!=null && stmp1.length()>0) { sbRemarks.append(stmp1+";\n"); }
		if(stmp2!=null && stmp2.length()>0) { sbRemarks.append(stmp2+";\n"); }
		
		int len = sbRemarks.length();
		if(len>0) {
			sbRemarks.delete(len-2, len);
		}
		
		return sbRemarks.length()>0?";"+sbRemarks.toString():"";
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

	/* ignoring whitespaces */
	@Override
	public boolean equals4Diff(Object obj) {
		if(obj instanceof View) {
			View v = (View) obj;
			//return name.equals(v.name) && query.equals(v.query);
			return name.equals(v.name) && DBObjectUtils.equalsIgnoreWhitespacesEachLine(query, v.query);
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((query == null) ? 0 : query.hashCode());
		return result;
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
	
	@Override
	public String getRelationType() {
		return "view";
	}
	
	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public CheckOptionType getCheckOption() {
		return checkOption;
	}

	public void setCheckOption(CheckOptionType checkOption) {
		this.checkOption = checkOption;
	}

	public boolean isWithReadOnly() {
		return withReadOnly;
	}

	public void setWithReadOnly(boolean withReadOnly) {
		this.withReadOnly = withReadOnly;
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
	
	public void setSimpleColumns(List<Column> cols) {
		if(cols==null) {
			this.columns = null;
			return;
		}
		
		this.columns = new ArrayList<Column>();
		for(int i=0;i<cols.size();i++) {
			Column tcol = cols.get(i);
			Column c = new Column();
			c.setName(tcol.getName());
			c.setRemarks(tcol.getRemarks());
			this.columns.add(c);
		}
	}

	public void setConstraints(List<Constraint> constraints) {
		this.constraints = constraints;
	}

	@Override
	public Integer getParameterCount() {
		return parameterCount;
	}

	public void setParameterCount(Integer parameterCount) {
		this.parameterCount = parameterCount;
	}
	
	/*@Override
	public String getAfterCreateScript() {
		return null;
	}*/
}
