package tbrugz.sqldump;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;

public class SchemaModelScriptDumper {
	
	FileWriter fos;
	boolean dumpWithSchemaName;
	
	public SchemaModelScriptDumper(FileWriter fos) {
		this.fos = fos;
	}

	public boolean isDumpWithSchemaName() {
		return dumpWithSchemaName;
	}

	public void setDumpWithSchemaName(boolean dumpWithSchemaName) {
		this.dumpWithSchemaName = dumpWithSchemaName;
	}

	void dumpFKsOutsideTable(Collection<FK> foreignKeys) throws IOException {
		for(FK fk: foreignKeys) {
			out("alter table "+(dumpWithSchemaName?fk.fkTableSchemaName+".":"")+fk.fkTable
				+"\n\tadd constraint "+fk.name
				+" foreign key ("+SQLDataDump.join(fk.fkColumns, ", ")+
				")\n\treferences "+(dumpWithSchemaName?fk.pkTableSchemaName+".":"")+fk.pkTable+" ("+SQLDataDump.join(fk.pkColumns, ", ")+");\n");
		}
	}
	
	void dumpFKsInsideTable(Collection<FK> foreignKeys) throws IOException {
		for(FK fk: foreignKeys) {
			out("\tconstraint "+fk.name+" foreign key ("+SQLDataDump.join(fk.fkColumns, ", ")
				+") references "+(dumpWithSchemaName?fk.pkTableSchemaName+".":"")+fk.pkTable+" ("+SQLDataDump.join(fk.pkColumns, ", ")+"),\n");
		}
	}
	
	void out(String s) throws IOException {
		fos.write(s+"\n");
	}
	
}
