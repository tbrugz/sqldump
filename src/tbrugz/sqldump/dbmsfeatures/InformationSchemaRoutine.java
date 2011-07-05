package tbrugz.sqldump.dbmsfeatures;

import tbrugz.sqldump.dbmodel.ExecutableObject;

public class InformationSchemaRoutine extends ExecutableObject {
	public String returnType;
	public String externalLanguage;

	@Override
	public String getDefinition(boolean dumpSchemaName) {
		return "create or replace "+type+" "+name+"()\n  returns "+returnType+" as \n$BODY$"
				+body+"$BODY$"
				+"\n  language "+externalLanguage+";";
	}
}
