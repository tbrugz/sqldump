package tbrugz.sqldump;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

/*
 * TODO: compact Grant dump
 */
public class SchemaModelScriptDumper {
	
	FileWriter fos;
	boolean dumpWithSchemaName;
	boolean dumpPKs;
	boolean dumpFKsInsideTable;
	Properties columnTypeMapping;
	String fromDbId, toDbId;
	
	
	public SchemaModelScriptDumper(FileWriter fos) {
		this.fos = fos;
	}

	public boolean isDumpWithSchemaName() {
		return dumpWithSchemaName;
	}

	public void setDumpWithSchemaName(boolean dumpWithSchemaName) {
		this.dumpWithSchemaName = dumpWithSchemaName;
	}

	void dumpSchema(SchemaModel schemaModel) throws Exception {
		System.out.println("from: "+fromDbId+"; to: "+toDbId+"\n->"+columnTypeMapping);
		
		StringBuffer sb = new StringBuffer();
		for(Table table: schemaModel.tables) {
			sb.setLength(0);
			List<String> pkCols = new ArrayList<String>();
			
			String tableName = (dumpWithSchemaName?table.schemaName+".":"")+table.name;
			
			//Table
			sb.append("--drop table "+tableName+";\n");
			sb.append("create table "+tableName+" ( -- type="+table.type+"\n");
			//Columns
			for(Column c: table.columns) {
				String colDesc = SQLDataDump.getColumnDesc(c, columnTypeMapping, fromDbId, toDbId);
				if(c.pk) { pkCols.add(c.name); }
				sb.append("\t"+colDesc+",\n");
			}
			//PKs
			if(dumpPKs && pkCols.size()>0) {
				sb.append("\tconstraint "+table.pkConstraintName+" primary key ("+Utils.join(pkCols, ", ")+"),\n");
			}
			//FKs?
			if(dumpFKsInsideTable) {
				sb.append(dumpFKsInsideTable(schemaModel.foreignKeys, table.schemaName, table.name));
			}
			//Table end
			sb.delete(sb.length()-2, sb.length());
			sb.append("\n);\n");
			
			//Grants
			for(Grant grant: table.grants) {
				sb.append("grant "+grant.privilege
						+" on "+tableName
						+" to "+grant.grantee
						+(grant.withGrantOption?" WITH GRANT OPTION":"")
						+";\n");
			}
			out(sb.toString());
		}
		
		//FKs
		if(!dumpFKsInsideTable) {
			dumpFKsOutsideTable(schemaModel.foreignKeys);
		}
	}
	
	void dumpFKsOutsideTable(Collection<FK> foreignKeys) throws IOException {
		for(FK fk: foreignKeys) {
			out("alter table "+(dumpWithSchemaName?fk.fkTableSchemaName+".":"")+fk.fkTable
				+"\n\tadd constraint "+fk.name
				+" foreign key ("+Utils.join(fk.fkColumns, ", ")+
				")\n\treferences "+(dumpWithSchemaName?fk.pkTableSchemaName+".":"")+fk.pkTable+" ("+Utils.join(fk.pkColumns, ", ")+");\n");
		}
	}
	
	String dumpFKsInsideTable(Collection<FK> foreignKeys, String schemaName, String tableName) throws IOException {
		StringBuffer sb = new StringBuffer();
		for(FK fk: foreignKeys) {
			if(schemaName.equals(fk.fkTableSchemaName) && tableName.equals(fk.fkTable)) {
				sb.append("\tconstraint "+fk.name+" foreign key ("+Utils.join(fk.fkColumns, ", ")
					+") references "+(dumpWithSchemaName?fk.pkTableSchemaName+".":"")+fk.pkTable+" ("+Utils.join(fk.pkColumns, ", ")+"),\n");
			}
		}
		return sb.toString();
	}
	
	void out(String s) throws IOException {
		fos.write(s+"\n");
	}
	
}
