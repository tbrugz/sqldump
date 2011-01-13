package tbrugz.sqldump;

import java.util.HashSet;
import java.util.Set;

public class FK {
	String name;
	String pkTable;
	String fkTable;

	Set<String> pkColumns = new HashSet<String>();
	Set<String> fkColumns = new HashSet<String>();
	
	@Override
	public String toString() {
		return name+"["+fkTable+"<-"+pkTable+"]";
		//return "fk:"+name+"["+fkTable+"<-"+pkTable+"]";
	}
}
