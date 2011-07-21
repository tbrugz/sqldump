package tbrugz.sqldump.dbmodel;

import java.io.Serializable;
import java.util.Set;
import java.util.TreeSet;

import tbrugz.sqldump.Utils;

//XXX~: extends DBObject?
public class FK extends DBIdentifiable implements Comparable<FK>, Serializable {
	private static final long serialVersionUID = 1L;
	//String name;
	public String pkTable;
	public String fkTable;
	public String pkTableSchemaName;
	public String fkTableSchemaName;

	public Set<String> pkColumns = new TreeSet<String>(); //XXX: should be List<String>?
	public Set<String> fkColumns = new TreeSet<String>(); //should be List<String>?
	
	@Override
	public String toString() {
		return name+"["+fkTable+"<-"+pkTable+"]";
		//return "fk:"+name+"["+fkTable+"<-"+pkTable+"]";
	}

	/*public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}*/

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

	public static String fkSimpleScript(FK fk, String whitespace, boolean dumpWithSchemaName) {
		whitespace = whitespace.replaceAll("[^ \n\t]", " ");
		return "constraint "+fk.getName()
			+" foreign key ("+Utils.join(fk.fkColumns, ", ")+
			")"+whitespace+"references "+(dumpWithSchemaName?fk.pkTableSchemaName+".":"")+fk.pkTable+" ("+Utils.join(fk.pkColumns, ", ")+")";
	}

	@Override
	public String getDefinition(boolean dumpSchemaName) {
		return fkSimpleScript(this, " ", true);
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof FK) {
			FK fk = (FK) obj;
			return pkTable.equals(fk.pkTable) && fkTable.equals(fk.fkTable) 
					&& pkColumns.equals(fk.pkColumns) && fkColumns.equals(fk.fkColumns);
		}
		return false;
	}
	
}
