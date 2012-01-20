package tbrugz.sqldump.dbmodel;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;

import tbrugz.sqldump.DBMSResources;
import tbrugz.sqldump.SQLDump;
import tbrugz.sqldump.dbmodel.Constraint.ConstraintType;

public class Table extends DBObject {
	TableType type;
	List<Column> columns = new ArrayList<Column>();
	List<Grant> grants = new ArrayList<Grant>();
	List<Constraint> constraints = new ArrayList<Constraint>();
	String remarks;	//e.g. COMMENT ON TABLE ZZZ IS 'bla bla';
	Boolean domainTable;
	
	static Logger log = Logger.getLogger(Table.class);
	
	public Column getColumn(String name) {
		if(name==null) return null;
		for(Column c: columns) {
			if(name.equals(c.name)) return c;
		}
		return null;
	}
	
	@Override
	public String toString() {
		return type+":"+name;
		//return "Table[name:"+name+"]";
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
		return getDefinition(dumpSchemaName, true, false, false, null, null);
	}
	
	public String getDefinition(boolean dumpWithSchemaName, boolean dumpPKs, boolean dumpFKsInsideTable, boolean dumpDropStatements, Properties colTypeConversionProp, Set<FK> foreignKeys) {
		//List<String> pkCols = new ArrayList<String>();
		String tableName = (dumpWithSchemaName?schemaName+".":"")+name;
		
		StringBuffer sb = new StringBuffer();
		//Table
		if(dumpDropStatements) {
			sb.append("drop table "+tableName+";\n\n");
		}
		sb.append("create ");
		sb.append(getTableType4sql());
		sb.append("table "+tableName+" ( -- type="+type);

		int countTabElements=0;
		
		//Columns
		for(Column c: columns) {
			String colDesc = null;
			if(colTypeConversionProp!=null) {
				colDesc = Column.getColumnDesc(c, colTypeConversionProp, DBMSResources.instance().dbid(), colTypeConversionProp.getProperty(SQLDump.PROP_TO_DB_ID));
			}
			else {
				colDesc = Column.getColumnDesc(c, null, null, null);
			}
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
					if(!dumpPKs) break;
				case CHECK:
				case UNIQUE:
					sb.append((countTabElements==0?"":",")+"\n\t"+cons.getDefinition(false));
			}
			countTabElements++;
		}
		
		//FKs?
		if(dumpFKsInsideTable) {
			/*String fksDefinition = getFKsDefinition(dumpWithSchemaName, tableName, foreignKeys);
			if(fksDefinition!=null && !fksDefinition.equals("")) {
				sb.append(fksDefinition);
				countTabElements++; // + 'n'?
			}*/
			//sb.append(+"\n\t"+dumpFKsInsideTable(foreignKeys, schemaName, name, dumpWithSchemaName));
			for(FK fk: foreignKeys) {
				if((schemaName==null || fk.fkTableSchemaName==null || schemaName.equals(fk.fkTableSchemaName)) && name.equals(fk.fkTable)) {
					sb.append((countTabElements==0?"":",")+"\n\t"+FK.fkSimpleScript(fk, " ", dumpWithSchemaName));
				}
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
	
	/*String dumpFKsInsideTable(Collection<FK> foreignKeys, String schemaName, String tableName, boolean dumpWithSchemaName) {
		StringBuffer sb = new StringBuffer();
		for(FK fk: foreignKeys) {
			if(schemaName.equals(fk.fkTableSchemaName) && tableName.equals(fk.fkTable)) {
				//sb.append("\tconstraint "+fk.getName()+" foreign key ("+Utils.join(fk.fkColumns, ", ")
				//	+") references "+(dumpWithSchemaName?fk.pkTableSchemaName+".":"")+fk.pkTable+" ("+Utils.join(fk.pkColumns, ", ")+"),\n");
				//sb.append("\t"+FK.fkSimpleScript(fk, " ", dumpWithSchemaName)+",\n");
				sb.append("\t"+FK.fkSimpleScript(fk, " ", dumpWithSchemaName)+",\n");
			}
		}
		return sb.toString();
	}*/
	
	public String getTableType4sql() {
		return "";
	}

	public String getTableFooter4sql() {
		return "";
	}
	
	public String getAfterCreateTableScript() {
		//e.g.: COMMENT ON COLUMN [schema.]table.column IS 'text'
		StringBuffer sb = new StringBuffer();
		String tableComment = getRemarks();
		if(tableComment!=null && !tableComment.trim().equals("")) {
			tableComment = tableComment.replaceAll("'", "''");
			sb.append("comment on table "+schemaName+"."+name+" is '"+tableComment+"';\n");
		}
		//XXX: column comments should be ordered by col name?
		for(Column c: getColumns()) {
			String comment = c.getRemarks();
			if(comment!=null && !comment.trim().equals("")) {
				//XXXdone: escape comment
				comment = comment.replaceAll("'", "''");
				sb.append("comment on column "+schemaName+"."+name+"."+c.name+" is '"+comment+"';\n");
			}
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

	public List<Grant> getGrants() {
		return grants;
	}

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

	public String getRemarks() {
		return remarks;
	}

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
	
}
