package tbrugz.sqldump.dbmsfeatures;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import tbrugz.sqldump.dbmodel.ExecutableObject;

//TODO: remove returnType, parameterNames & parameterTypes (use the ones from ExecutableObject)
public class InformationSchemaRoutine extends ExecutableObject {
	private static final long serialVersionUID = 1L;
	
	public String returnType;
	public String externalLanguage;
	public List<String> parameterNames = new ArrayList<String>();
	public List<String> parameterTypes = new ArrayList<String>();

	static final Pattern PATTERN_CREATE_EXECUTABLE = Pattern.compile("\\s*create\\s+", Pattern.CASE_INSENSITIVE);
	
	@Override
	public String getDefinition(boolean dumpSchemaName) {
		if(PATTERN_CREATE_EXECUTABLE.matcher(getBody()).find()) {
			return getBody();
		}
		
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
		
		return "create or replace "+getType()+" "+getName()+"("
				+(sb!=null?sb.toString():"")
				+")\n  returns "+returnType+" as \n$BODY$"
				+getBody()+"$BODY$"
				+"\n  language "+externalLanguage+";";
	}
}
