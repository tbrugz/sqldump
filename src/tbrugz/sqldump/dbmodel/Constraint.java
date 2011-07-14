package tbrugz.sqldump.dbmodel;

public class Constraint implements Comparable<Constraint> {
	
	public static enum ConstraintType {
		CHECK,  //CONSTRAINT check1 CHECK (char_length("EMAIL") > 5); 
		UNIQUE; //CONSTRAINT unique1 UNIQUE("MANAGER_NAME", "EMAIL");
	}

	public ConstraintType type;
	public String name;
	public String description;
	
	public String getDefinition(boolean dumpSchemaName) {
		return "constraint "+name+" "+type+" "+description;
	}
	
	public int compareTo(Constraint c) {
		if(type.equals(c.type)) {
			return name.compareTo(c.name);
		}
		return type.compareTo(c.type);
	}
}
