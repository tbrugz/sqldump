package tbrugz.sqldump.dbmsfeatures;

import tbrugz.sqldump.dbmodel.FK;

public class OracleFK extends FK {
	private static final long serialVersionUID = 1L;
	
	public Boolean enabled; // enabled | disabled
	public Boolean validated;
	public Boolean rely;
	
	@Override
	public String fkSimpleScript(String whitespace, boolean dumpWithSchemaName) {
		return super.fkSimpleScript(whitespace, dumpWithSchemaName)
			+ (enabled!=null && !enabled?" disable":"")
			+ (validated!=null && !validated?" novalidate":"")
			+ (rely!=null && !rely?" norely":"");
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
