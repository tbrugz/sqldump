package tbrugz.sqldump.dbmodel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import tbrugz.sqldump.Utils;

public class Constraint implements Comparable<Constraint>, Serializable {
	private static final long serialVersionUID = 1L;

	public static enum ConstraintType {
		PK,     //CONSTRAINT pk1 PRIMARY KEY ("MANAGER_NAME", "EMAIL");
		UNIQUE, //CONSTRAINT unique1 UNIQUE ("MANAGER_NAME", "EMAIL");
		CHECK;  //CONSTRAINT check1 CHECK (char_length("EMAIL") > 5); 
		
		public String fullName() {
			switch (this) {
				case PK:
					return "PRIMARY KEY";
				case CHECK:
				case UNIQUE:
				default:
					return this.toString();
			}
		}
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
			case PK:
				return "constraint "+name+" "+type.fullName()+" ("+Utils.join(uniqueColumns, ", ")+")";
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
