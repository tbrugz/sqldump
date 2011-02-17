package tbrugz.sqldump.dbmodel;

import java.util.HashSet;
import java.util.Set;

//XXX~: extends DBObject?
public class FK implements Comparable<FK>{
	String name;
	public String pkTable;
	public String fkTable;
	public String pkTableSchemaName;
	public String fkTableSchemaName;

	public Set<String> pkColumns = new HashSet<String>();
	public Set<String> fkColumns = new HashSet<String>();
	
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

	public int compareTo(FK o) {
		int fkCompare = fkTable.compareTo(o.fkTable);
		if(fkCompare==0) { //if same FK Table, compare FK Name
			return name.compareTo(o.name);
		}
		return fkCompare;
		// return name.compareTo(o.name);
	}
}
