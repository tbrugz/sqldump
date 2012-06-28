package tbrugz.sqldump.dbmsfeatures;

import java.util.ArrayList;
import java.util.List;

import tbrugz.sqldump.dbmodel.ExecutableObject;

public class InformationSchemaRoutine extends ExecutableObject {
	public String returnType;
	public String externalLanguage;
	public List<String> parameters = new ArrayList<String>();

	@Override
	public String getDefinition(boolean dumpSchemaName) {
		StringBuffer sb = null;
		if(parameters!=null) {
			sb = new StringBuffer();
			for(int i=0;i<parameters.size();i++) {
				if(i>0) { sb.append(", "); }
				sb.append(parameters.get(i));
			}
		}
		
		return "create or replace "+type+" "+name+"("
				+(sb!=null?sb.toString():"")
				+")\n  returns "+returnType+" as \n$BODY$"
				+body+"$BODY$"
				+"\n  language "+externalLanguage+";";
	}
}
