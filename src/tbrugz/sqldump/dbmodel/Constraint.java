package tbrugz.sqldump.dbmodel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import tbrugz.sqldump.Utils;
import tbrugz.sqldump.util.SQLIdentifierDecorator;

public class Constraint extends DBIdentifiable implements Comparable<Constraint>, Serializable {
	private static final long serialVersionUID = 1L;

	public static enum ConstraintType {
		PK,     //CONSTRAINT pk1 PRIMARY KEY ("MANAGER_NAME", "EMAIL");
		UNIQUE, //CONSTRAINT unique1 UNIQUE ("MANAGER_NAME", "EMAIL");
		CHECK;  //CONSTRAINT check1 CHECK (char_length("EMAIL") > 5); 
		
		public String fullName() {
			switch (this) {
				case PK:
					//return "PRIMARY KEY";
					return "primary key";
				case CHECK:
				case UNIQUE:
				default:
					return this.toString();
			}
		}
	}

	public ConstraintType type;
	//public String name;
	public String checkDescription;
	public List<String> uniqueColumns = new ArrayList<String>();
	
	public String getDefinition(boolean dumpSchemaName) {
		switch (type) {
			//XXX: use literal type (CHECK, UNIQUE) instead of variable 'type'?
			case CHECK:
				return "constraint "+DBObject.getFinalIdentifier(name)+" "+type+" "+checkDescription;
			case UNIQUE:
			case PK:
				return "constraint "+DBObject.getFinalIdentifier(name)+" "+type.fullName()+" ("+Utils.join(uniqueColumns, ", ", SQLIdentifierDecorator.getInstance())+")";
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
		//return getDefinition(false);
		return "["+type+":"+name+"]";
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Constraint) {
			Constraint cc = (Constraint) obj;
			if(!name.equalsIgnoreCase(cc.name)) { return false; }
			if(type.equals(cc.type)) {
				switch (type) {
				case CHECK:
					return checkDescription.equalsIgnoreCase(cc.checkDescription);
				case UNIQUE:
				case PK:
					//return uniqueColumns.equals(cc.uniqueColumns);
					return Utils.stringListEqualIgnoreCase(uniqueColumns, cc.uniqueColumns);
				}
			}
			else return false;
		}
		return false;
	}
}
