package tbrugz.sqldump.dbmsfeatures;

import tbrugz.sqldump.dbmodel.FK;

public class OracleFK extends FK {
	Boolean enabled; // enabled | disabled
	Boolean validated;
	Boolean rely;
	
	@Override
	public String fkSimpleScript(String whitespace, boolean dumpWithSchemaName) {
		return super.fkSimpleScript(whitespace, dumpWithSchemaName)
			+ (enabled!=null && !enabled?" disable":"")
			+ (validated!=null && !validated?" novalidate":"")
			+ (rely!=null && !rely?" norely":"")
			;
		//return super.getDefinition(dumpSchemaName)
	};
	
	@Override
	public String toString() {
		return super.toString()+" ["
			+(enabled!=null&&!enabled?" disable;":"")
			+(validated!=null&&!validated?" novalidate;":"")
			+(rely!=null&&!rely?" norely;":"")
			+"]";
	}
}
