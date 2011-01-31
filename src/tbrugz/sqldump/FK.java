package tbrugz.sqldump;

import java.util.HashSet;
import java.util.Set;

public class FK {
	String name;
	String pkTable;
	String fkTable;
	String pkTableSchemaName;
	String fkTableSchemaName;

	Set<String> pkColumns = new HashSet<String>();
	Set<String> fkColumns = new HashSet<String>();
	
	@Override
	public String toString() {
		return name+"["+fkTable+"<-"+pkTable+"]";
		//return "fk:"+name+"["+fkTable+"<-"+pkTable+"]";
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getSourceId() {
		return pkTableSchemaName+"."+pkTable;
	}

	public String getTargetId() {
		return fkTableSchemaName+"."+fkTable;
	}

}
