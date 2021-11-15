package tbrugz.sqldump.dbmodel;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.Constraint.ConstraintType;
import tbrugz.sqldump.util.Utils;

public class Table extends DBObject implements Relation {
	
	private static final long serialVersionUID = 1L;

	TableType type = TableType.TABLE;
	List<Column> columns = new ArrayList<Column>();
	List<Grant> grants = new ArrayList<Grant>();
	List<Constraint> constraints = new ArrayList<Constraint>();
	List<FK> foreignKeys = new ArrayList<FK>();
	String remarks; //e.g. COMMENT ON TABLE ZZZ IS 'bla bla';
	Boolean domainTable;
	
	static final Log log = LogFactory.getLog(Table.class);
	
	public Column getColumn(String name) {
		if(name==null) { return null; }
		for(Column c: columns) {
			if(name.equals(c.name)) { return c; }
		}
		return null;
	}
	
	public Column getColumnIgnoreCase(String name) {
		if(name==null) { return null; }
		for(Column c: columns) {
			if(name.toUpperCase().equals(c.name.toUpperCase())) { return c; }
		}
		return null;
	}
	
	@Override
	public String toString() {
		return type+":"+(schemaName!=null?schemaName+".":"")+name;
	}
	
	public void validateConstraints() {
		Constraint cPK = getPKConstraint();
		if(cPK==null) {
			log.debug("table "+name+" ["+type+"] has no PK");
			return;
		}
		for(Column c: columns) {
			if(cPK.uniqueColumns.contains(c.name)) {
				c.pk = true;
				//log.info("table "+name+" pkcol: "+c);
			}
		}
	}

	@Override
	public String getDefinition(boolean dumpSchemaName) {
		Set<FK> fks = null;
		boolean dumpFKs = false;
		if(foreignKeys!=null && foreignKeys.size()>0) {
			dumpFKs = true;
			fks = new TreeSet<FK>();
			fks.addAll(foreignKeys);
		}
		return getDefinition(dumpSchemaName, true, dumpFKs, false, true, fks);
	}
	
	public String getDefinition(boolean dumpWithSchemaName, boolean dumpPKs, boolean dumpFKsInsideTable, boolean dumpDropStatements, boolean dumpComments,
			Set<FK> foreignKeys) {
		//List<String> pkCols = new ArrayList<String>();
		String tableName = getFinalName(dumpWithSchemaName);

		StringBuilder sb = new StringBuilder();
		//Table
		if(dumpDropStatements) {
			sb.append("drop table "+tableName+";\n\n");
		}
		sb.append("create ");
		String type4sql = getTableType4sql();
		sb.append(type4sql);
		if(type4sql==null || !type4sql.isEmpty()) { sb.append(" "); }
		sb.append("table "+tableName);

		List<String> elements = new ArrayList<>();
		
		//Columns
		if(shouldDumpInnerElements()) {

		for(Column c: columns) {
			String colDesc = c.getDefinition();
			//if(c.pk) { pkCols.add(c.name); }
			elements.add(colDesc);
		}
		
		//PKs
		/*if(dumpPKs && pkCols.size()>0) {
			sb.append("\tconstraint "+pkConstraintName+" primary key ("+Utils.join(pkCols, ", ")+"),\n");
		}*/
		
		/*String constraintDef = getConstraintsDefinition(dumpPKs);
		if(constraintDef!=null && !constraintDef.equals("")) {
			sb.append(constraintDef);
			countTabElements++; // + 'n'?
		}*/
		
		//Constraints (CHECK, UNIQUE)
		for(Constraint cons: constraints) {
			switch(cons.type) {
				case PK: 
					if(!dumpPKs) { break; }
				case CHECK:
				case UNIQUE:
					elements.add(cons.getDefinition(false));
				default:
					break;
			}
		}
		
		//FKs?
		if(dumpFKsInsideTable) {
			List<FK> fks = ModelUtils.getImportedKeys(this, foreignKeys);
			for(FK fk: fks) {
				elements.add(fk.fkSimpleScript(" ", dumpWithSchemaName));
			}
		}

		}
		
		//Table end
		if(elements.size()==0) {
			sb.append(dumpComments?" /* type="+type+" */":"");
		}
		else {
			sb.append(" (" + (dumpComments?" -- type="+type:"") + "\n\t");
			sb.append(Utils.join(elements, ",\n\t"));
			sb.append("\n)");
		}
		//sb.delete(sb.length()-2, sb.length());
		sb.append(getTableFooter4sql());
		return sb.toString();
	}
	
	public boolean shouldDumpInnerElements() {
		return true;
	}
	
	/*public String getConstraintsDefinition(boolean dumpPK) {
		//Constraints (PK, CHECK, UNIQUE)
		StringBuffer sb = new StringBuffer();
		int count = 0;
		for(Constraint cons: constraints) {
			switch(cons.type) {
				case PK: 
					if(!dumpPK) break;
				case CHECK:
				case UNIQUE:
					sb.append((count==0?"":",")+"\n\t"+cons.getDefinition(false));
					count++;
			}
		}
		return sb.toString();
	}

	public String getFKsDefinition(boolean dumpWithSchemaName, String tableName, Set<FK> foreignKeys) {
		StringBuffer sb = new StringBuffer();
		int count = 0;
		//sb.append(+"\n\t"+dumpFKsInsideTable(foreignKeys, schemaName, name, dumpWithSchemaName));
		for(FK fk: foreignKeys) {
			if(schemaName.equals(fk.fkTableSchemaName) && tableName.equals(fk.fkTable)) {
				sb.append((count==0?"":",")+"\n\t"+FK.fkSimpleScript(fk, " ", dumpWithSchemaName));
			}
			count++;
		}
		return sb.toString();
	}*/
	
	public String getTableType4sql() {
		return "";
	}

	public String getTableFooter4sql() {
		return "";
	}
	
	@Override
	public String getRemarksSnippet(boolean dumpSchemaName) {
		return getAfterCreateTableScript(dumpSchemaName, true);
	}
	
	public String getAfterCreateTableScript(boolean dumpSchemaName, boolean dumpRemarks) {
		//e.g.: COMMENT ON COLUMN [schema.]table.column IS 'text'
		StringBuilder sb = new StringBuilder();
		if(dumpRemarks) {
			String stmp = getRelationRemarks(this, dumpSchemaName);
			if(stmp!=null && stmp.length()>0) { sb.append(stmp+";\n"); }

			stmp = getColumnRemarks(columns, this, dumpSchemaName);
			if(stmp!=null && stmp.length()>0) { sb.append(stmp+";\n"); }
		}

		return sb.toString();
	}
	
	public Constraint getPKConstraint() {
		//validateConstraints();
		for(Constraint c: constraints) {
			if(c.type.equals(ConstraintType.PK)) {
				return c;
			}
		}
		return null;
	}
	
	//---------
	
	@Override
	public String getRelationType() {
		return type.getName();
	}
	
	//XXX: rename to getTableType()?
	public TableType getType() {
		return type;
	}

	public void setType(TableType type) {
		this.type = type;
	}

	public List<Column> getColumns() {
		return columns;
	}

	public void setColumns(List<Column> columns) {
		this.columns = columns;
	}

	@Override
	public List<Grant> getGrants() {
		return grants;
	}

	@Override
	public void setGrants(List<Grant> grants) {
		this.grants = grants;
	}

	/*
	//XXX: getPkConstraintName should be @XmlTransient?
	@XmlTransient
	public String getPkConstraintName() {
		return pkConstraintName;
	}

	public void setPkConstraintName(String pkConstraintName) {
		this.pkConstraintName = pkConstraintName;
	}
	*/

	public List<Constraint> getConstraints() {
		return constraints;
	}

	public void setConstraints(List<Constraint> constraints) {
		this.constraints = constraints;
	}

	@Override
	public String getRemarks() {
		return remarks;
	}

	@Override
	public void setRemarks(String remarks) {
		this.remarks = remarks;
	}

	public Boolean getDomainTable() {
		return domainTable;
	}

	public void setDomainTable(Boolean domainTable) {
		this.domainTable = domainTable;
	}

	//public boolean isDomainTable() - jaxb is not happy with this - conflicts with get/set
	public boolean isTableADomainTable() {
		return domainTable!=null && domainTable;
	}

	@Override
	public List<String> getColumnNames() {
		return getColumnNames(columns);
	}

	@Override
	public List<String> getColumnTypes() {
		return getColumnTypes(columns);
	}
	
	@Override
	public List<String> getColumnRemarks() {
		return getColumnRemarks(columns);
	}
	
	public static List<String> getColumnNames(List<Column> columns) {
		if(columns==null) { return null; }
		List<String> ret = new ArrayList<String>();
		for(Column c: columns) {
			ret.add(c.getName());
		}
		return ret;
	}

	static List<String> getColumnTypes(List<Column> columns) {
		if(columns==null) { return null; }
		List<String> ret = new ArrayList<String>();
		for(Column c: columns) {
			ret.add(c.getType());
		}
		return ret;
	}
	
	public static List<String> getColumnRemarks(List<Column> columns) {
		if(columns==null) { return null; }
		List<String> ret = new ArrayList<String>();
		for(Column c: columns) {
			ret.add(c.getRemarks());
		}
		return ret;
	}
	
	public static String getRelationRemarks(Relation rel, boolean dumpSchemaName) {
		return getRelationRemarks(rel, dumpSchemaName, false);
	}
	
	public static String getRelationRemarks(Relation rel, boolean dumpSchemaName, boolean dumpIfNull) {
		StringBuilder sb = new StringBuilder();
		String tableComment = rel.getRemarks();
		if(dumpIfNull || (tableComment!=null && !tableComment.trim().equals(""))) {
			sb.append(getRemarksSql(rel, tableComment, dumpSchemaName));
			//sb.append("comment on "+rel.getRelationType()+" "+DBObject.getFinalName(rel, dumpSchemaName)+" is '"+tableComment+"'"); //;\n
		}
		return sb.toString();
	}
	
	public static String getRemarksSql(Relation rel, String remarks, boolean dumpSchemaName) {
		if(remarks==null) { remarks = ""; }
		remarks = remarks.replaceAll("'", "''");
		return "comment on "+rel.getRelationType()+" "+DBObject.getFinalName(rel, dumpSchemaName)+" is '"+remarks+"'";
	}
	
	static String getColumnRemarks(List<Column> columns, Relation rel, boolean dumpSchemaName) {
		StringBuilder sb = new StringBuilder();
		//XXX: column comments should be ordered by col name?
		int commentCount = 0;
		if(columns!=null) {
			for(Column c: columns) {
				String remarks = getColumnRemarks(rel, c, dumpSchemaName, false);
				if(remarks!=null) {
					if(commentCount>0) { sb.append(";\n"); }
					sb.append(remarks);
					commentCount++;
					//sb.append(";\n");
				}
			}
		}
		return sb.toString();
	}
	
	public static String getColumnRemarks(NamedDBObject rel, Column c, boolean dumpSchemaName, boolean dumpIfNull) {
		String comment = c.getRemarks();
		if(dumpIfNull || (comment!=null && !comment.trim().equals("")) ) {
			//XXXdone: escape comment
			if(comment==null) { comment = ""; }
			comment = comment.replaceAll("'", "''");
			return "comment on column "+DBObject.getFinalName(rel, dumpSchemaName)+"."+DBObject.getFinalIdentifier(c.name)+" is '"+comment+"'";
		}
		return null;
	}
	
	@Override
	public Integer getParameterCount() {
		return null;
	}

	@Override
	public int getColumnCount() {
		return columns!=null?columns.size():0;
	}
	
	@Override
	public List<String> getParameterTypes() {
		return null;
	}
	
	@Override
	public DBObjectType getDbObjectType() {
		return DBObjectType.TABLE;
	}

	@Override
	public DBObjectType getDBObjectType() {
		return getDbObjectType();
	}
	
	public List<FK> getForeignKeys() {
		return foreignKeys;
	}
	
	public void setForeignKeys(List<FK> foreignKeys) {
		this.foreignKeys = foreignKeys;
	}
	
	@Override
	public List<String> getNamedParameterNames() {
		return null;
	}

}
