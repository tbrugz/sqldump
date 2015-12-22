package tbrugz.sqldump.dbmsfeatures;

import java.util.regex.Pattern;

import tbrugz.sqldump.dbmodel.ExecutableObject;

public class InformationSchemaRoutine extends ExecutableObject {
	private static final long serialVersionUID = 1L;
	
	public String externalLanguage;

	static final Pattern PATTERN_CREATE_EXECUTABLE = Pattern.compile("\\s*create\\s+", Pattern.CASE_INSENSITIVE);
	
	@Override
	public String getDefinition(boolean dumpSchemaName) {
		if(PATTERN_CREATE_EXECUTABLE.matcher(getBody()).find()) {
			return getBody();
		}
		
		StringBuilder sb = null;
		if(params!=null) {
			sb = new StringBuilder();
			for(int i=0;i<params.size();i++) {
				if(i>0) { sb.append(", "); }
				sb.append(params.get(i).getName());
				sb.append(" ");
				sb.append(params.get(i).getDataType());
			}
		}
		
		return "create or replace "+getType()+" "+getName()+"("
				+(sb!=null?sb.toString():"")
				//+")\n  returns "+returnType+" as \n$BODY$"
				+")"
				+(returnParam!=null?"\n  returns "+returnParam.getDataType():"")
				+" as \n$BODY$"
				+getBody()+"$BODY$"
				+"\n  language "+externalLanguage+";";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime
				* result
				+ ((externalLanguage == null) ? 0 : externalLanguage.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		InformationSchemaRoutine other = (InformationSchemaRoutine) obj;
		if (externalLanguage == null) {
			if (other.externalLanguage != null)
				return false;
		} else if (!externalLanguage.equals(other.externalLanguage))
			return false;
		return true;
	}
	
}
