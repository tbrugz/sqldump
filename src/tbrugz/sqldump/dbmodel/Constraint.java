package tbrugz.sqldump.dbmodel;

import java.util.ArrayList;
import java.util.List;

import tbrugz.sqldump.Utils;

public class Constraint implements Comparable<Constraint> {
	
	public static enum ConstraintType {
		CHECK,  //CONSTRAINT check1 CHECK (char_length("EMAIL") > 5); 
		UNIQUE; //CONSTRAINT unique1 UNIQUE("MANAGER_NAME", "EMAIL");
	}

	public ConstraintType type;
	public String name;
	public String checkDescription;
	public List<String> uniqueColumns = new ArrayList<String>();
	
	public String getDefinition(boolean dumpSchemaName) {
		switch (type) {
			//XXX: use literal type (CHECK, UNIQUE) instead of variable 'type'?
			case CHECK:
				return "constraint "+name+" "+type+" "+checkDescription;
			case UNIQUE:
				return "constraint "+name+" "+type+" ("+Utils.join(uniqueColumns, ", ")+")";
		}
		throw new RuntimeException("unknown constraint type: "+type);
	}
	
	public int compareTo(Constraint c) {
		if(type.equals(c.type)) {
			return name.compareTo(c.name);
		}
		return type.compareTo(c.type);
	}
	
	@Override
	public String toString() {
		return "["+type+":"+name+"]";
	}
}
