package tbrugz.sqldump.dbmodel;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;

import tbrugz.sqldump.SQLDump;
import tbrugz.sqldump.dbmodel.Constraint.ConstraintType;

public class Table extends DBObject {
	TableType type;
	List<Column> columns = new ArrayList<Column>();
	List<Grant> grants = new ArrayList<Grant>();
	//String pkConstraintName;
	List<Constraint> constraints = new ArrayList<Constraint>();
	
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
		//return "t:"+name;
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
		// XXXxxx Table: getDefinition(dumpSchemaName, true, false, null, null)??
		return getDefinition(dumpSchemaName, true, false, null, null);
	}
	
	public String getDefinition(boolean dumpWithSchemaName, boolean dumpPKs, boolean dumpFKsInsideTable, Properties colTypeConversionProp, Set<FK> foreignKeys) {
		//List<String> pkCols = new ArrayList<String>();
		String tableName = (dumpWithSchemaName?schemaName+".":"")+name;
		
		StringBuffer sb = new StringBuffer();
		//Table
		sb.append("--drop table "+tableName+";\n");
		sb.append("create ");
		sb.append(getTableType4sql());
		sb.append("table "+tableName+" ( -- type="+type);

		int countTabElements=0;
		
		//Columns
		for(Column c: columns) {
			String colDesc = null;
			if(colTypeConversionProp!=null) {
				colDesc = Column.getColumnDesc(c, colTypeConversionProp, colTypeConversionProp.getProperty(SQLDump.PROP_FROM_DB_ID), colTypeConversionProp.getProperty(SQLDump.PROP_TO_DB_ID));
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
			//sb.append(+"\n\t"+dumpFKsInsideTable(foreignKeys, schemaName, name, dumpWithSchemaName));
			for(FK fk: foreignKeys) {
				if(schemaName.equals(fk.fkTableSchemaName) && tableName.equals(fk.fkTable)) {
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
		return ""; //"" or null
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
	
}
