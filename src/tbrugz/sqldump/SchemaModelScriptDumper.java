package tbrugz.sqldump;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.ExecutableObject;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.Grant;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.dbmodel.Trigger;
import tbrugz.sqldump.dbmodel.View;

/*
 * TODO: compact Grant dump
 */
public class SchemaModelScriptDumper extends SchemaModelDumper {
	
	static Logger log = Logger.getLogger(SchemaModelScriptDumper.class);

	File fileOutput;
	FileWriter fos;
	
	boolean dumpWithSchemaName;
	boolean dumpPKs;
	boolean dumpFKsInsideTable;
	boolean dumpSynonymAsTable;
	boolean dumpViewAsTable;
	
	Properties columnTypeMapping;
	String fromDbId, toDbId;
	
	
	/*public SchemaModelScriptDumper(FileWriter fos) {
		this.fos = fos;
	}*/
	
	@Override
	public void setOutput(File output) {
		this.fileOutput = output;
	}

	/* (non-Javadoc)
	 * @see tbrugz.sqldump.SchemaModelDumper#isDumpWithSchemaName()
	 */
	@Override
	public boolean isDumpWithSchemaName() {
		return dumpWithSchemaName;
	}

	/* (non-Javadoc)
	 * @see tbrugz.sqldump.SchemaModelDumper#setDumpWithSchemaName(boolean)
	 */
	@Override
	public void setDumpWithSchemaName(boolean dumpWithSchemaName) {
		this.dumpWithSchemaName = dumpWithSchemaName;
	}

	/* (non-Javadoc)
	 * @see tbrugz.sqldump.SchemaModelDumper#dumpSchema(tbrugz.sqldump.SchemaModel)
	 */
	@Override
	public void dumpSchema(SchemaModel schemaModel) throws Exception {
		fos = new FileWriter(fileOutput);
		
		log.debug("from: "+fromDbId+"; to: "+toDbId);
		log.debug("props->"+columnTypeMapping);
		
		StringBuffer sb = new StringBuffer();
		for(Table table: schemaModel.tables) {
			switch(table.type) {
				case SYNONYM: if(dumpSynonymAsTable) { break; } else { continue; } 
				case VIEW: if(dumpViewAsTable) { break; } else { continue; }
			}
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
		
		//Views
		for(View v: schemaModel.views) {
			out(v.getDefinition(dumpWithSchemaName)+"\n");
		}

		//Triggers
		for(Trigger t: schemaModel.triggers) {
			out(t.getDefinition(dumpWithSchemaName)+"\n");
		}

		//ExecutableObjects
		for(ExecutableObject eo: schemaModel.executables) {
			out("-- "+eo.type+" "+eo.name+"\n");
			out(eo.getDefinition(dumpWithSchemaName)+"\n");
		}
		
		fos.close();
	}
	
	void dumpFKsOutsideTable(Collection<FK> foreignKeys) throws IOException {
		for(FK fk: foreignKeys) {
			out("alter table "+(dumpWithSchemaName?fk.fkTableSchemaName+".":"")+fk.fkTable
				+"\n\tadd constraint "+fk.getName()
				+" foreign key ("+Utils.join(fk.fkColumns, ", ")+
				")\n\treferences "+(dumpWithSchemaName?fk.pkTableSchemaName+".":"")+fk.pkTable+" ("+Utils.join(fk.pkColumns, ", ")+");\n");
		}
	}
	
	String dumpFKsInsideTable(Collection<FK> foreignKeys, String schemaName, String tableName) throws IOException {
		StringBuffer sb = new StringBuffer();
		for(FK fk: foreignKeys) {
			if(schemaName.equals(fk.fkTableSchemaName) && tableName.equals(fk.fkTable)) {
				sb.append("\tconstraint "+fk.getName()+" foreign key ("+Utils.join(fk.fkColumns, ", ")
					+") references "+(dumpWithSchemaName?fk.pkTableSchemaName+".":"")+fk.pkTable+" ("+Utils.join(fk.pkColumns, ", ")+"),\n");
			}
		}
		return sb.toString();
	}
	
	void out(String s) throws IOException {
		fos.write(s+"\n");
	}
	
}
