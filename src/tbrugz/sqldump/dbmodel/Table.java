package tbrugz.sqldump.dbmodel;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.Constraint.ConstraintType;

public class Table extends DBObject implements Relation {
	private static final long serialVersionUID = 1L;

	TableType type = TableType.TABLE;
	List<Column> columns = new ArrayList<Column>();
	List<Grant> grants = new ArrayList<Grant>();
	List<Constraint> constraints = new ArrayList<Constraint>();
	String remarks; //e.g. COMMENT ON TABLE ZZZ IS 'bla bla';
	Boolean domainTable;
	
	static Log log = LogFactory.getLog(Table.class);
	
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
		return getDefinition(dumpSchemaName, true, false, false, true, null, null);
	}
	
	public String getDefinition(boolean dumpWithSchemaName, boolean dumpPKs, boolean dumpFKsInsideTable, boolean dumpDropStatements, boolean dumpComments, Properties colTypeConversionProp, Set<FK> foreignKeys) {
		//List<String> pkCols = new ArrayList<String>();
		String tableName = getFinalName(dumpWithSchemaName);

		StringBuffer sb = new StringBuffer();
		//Table
		if(dumpDropStatements) {
			sb.append("drop table "+tableName+";\n\n");
		}
		sb.append("create ");
		sb.append(getTableType4sql());
		sb.append("table "+tableName+" ("
				+(dumpComments?" -- type="+type:"") );

		int countTabElements=0;
		
		//Columns
		for(Column c: columns) {
			String colDesc = c.getDefinition();
			//if(c.pk) { pkCols.add(c.name); }
			sb.append((countTabElements==0?"":",")+"\n\t"+colDesc);
			countTabElements++;
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
					sb.append((countTabElements==0?"":",")+"\n\t"+cons.getDefinition(false));
				default:
					break;
			}
			countTabElements++;
		}
		
		//FKs?
		if(dumpFKsInsideTable) {
			List<FK> fks = ModelUtils.getImportedKeys(this, foreignKeys);
			for(FK fk: fks) {
				sb.append((countTabElements==0?"":",")+"\n\t"+fk.fkSimpleScript(" ", dumpWithSchemaName));
				countTabElements++;
			}
		}
		
		//Table end
		//sb.delete(sb.length()-2, sb.length());
		sb.append("\n)");
		sb.append(getTableFooter4sql());
		return sb.toString();
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
	
	public String getAfterCreateTableScript(boolean dumpSchemaName, boolean dumpRemarks) {
		//e.g.: COMMENT ON COLUMN [schema.]table.column IS 'text'
		StringBuffer sb = new StringBuffer();
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
	
	static List<String> getColumnNames(List<Column> columns) {
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
	
	static String getRelationRemarks(Relation rel, boolean dumpSchemaName) {
		StringBuffer sb = new StringBuffer();
		String tableComment = rel.getRemarks();
		if(tableComment!=null && !tableComment.trim().equals("")) {
			tableComment = tableComment.replaceAll("'", "''");
			sb.append("comment on "+rel.getRelationType()+" "+DBObject.getFinalName(rel, dumpSchemaName)+" is '"+tableComment+"'"); //;\n
		}
		return sb.toString();
	}
	
	static String getColumnRemarks(List<Column> columns, Relation rel, boolean dumpSchemaName) {
		StringBuffer sb = new StringBuffer();
		//XXX: column comments should be ordered by col name?
		int commentCount = 0;
		if(columns!=null) {
			for(Column c: columns) {
				String comment = c.getRemarks();
				if(comment!=null && !comment.trim().equals("")) {
					//XXXdone: escape comment
					comment = comment.replaceAll("'", "''");
					if(commentCount>0) { sb.append(";\n"); }
					sb.append("comment on column "+DBObject.getFinalName(rel, dumpSchemaName)+"."+DBObject.getFinalIdentifier(c.name)+" is '"+comment+"'");
					commentCount++;
				}
			}
		}
		return sb.toString();
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
	
}
