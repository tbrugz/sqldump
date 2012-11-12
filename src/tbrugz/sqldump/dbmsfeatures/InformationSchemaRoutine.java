package tbrugz.sqldump.dbmsfeatures;

import java.util.ArrayList;
import java.util.List;

import tbrugz.sqldump.dbmodel.ExecutableObject;

public class InformationSchemaRoutine extends ExecutableObject {
	public String returnType;
	public String externalLanguage;
	public List<String> parameterNames = new ArrayList<String>();
	public List<String> parameterTypes = new ArrayList<String>();

	@Override
	public String getDefinition(boolean dumpSchemaName) {
		StringBuffer sb = null;
		if(parameterNames!=null) {
			sb = new StringBuffer();
			for(int i=0;i<parameterNames.size();i++) {
				if(i>0) { sb.append(", "); }
				sb.append(parameterNames.get(i));
				sb.append(" ");
				sb.append(parameterTypes.get(i));
			}
		}
		
		return "create or replace "+type+" "+getName()+"("
				+(sb!=null?sb.toString():"")
				+")\n  returns "+returnType+" as \n$BODY$"
				+body+"$BODY$"
				+"\n  language "+externalLanguage+";";
	}
}
