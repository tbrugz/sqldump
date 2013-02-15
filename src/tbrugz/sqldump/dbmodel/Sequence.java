package tbrugz.sqldump.dbmodel;

/*
 * see: http://download.oracle.com/docs/cd/E14072_01/server.112/e10592/statements_6015.htm
 */
public class Sequence extends DBObject {
	private static final long serialVersionUID = 1L;

	public Long minValue;
	public Long maxValue; //XXX: not used yet
	public long incrementBy;
	public transient long lastNumber;
	
	public static transient boolean dumpStartWith = false;

	@Override
	public String getDefinition(boolean dumpSchemaName) {
		//XXX: option to use "create or replace" for sequence?
		//return (dumpCreateOrReplace?"create or replace ":"create ") 
		return "create " 
			+"sequence "+getFinalName(dumpSchemaName)
			+(minValue!=null?" minvalue "+minValue:"")
			+(dumpStartWith?" start with "+lastNumber:"")
			+" increment by "+incrementBy;
	}
	
	@Override
	public String toString() {
		return "[Sequence:"+getSchemaName()+"."+getName()+";min:"+minValue+",max:"+maxValue+"]";
	}

	//XXX: do not use lastNumber
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		Sequence other = (Sequence) obj;
		if (incrementBy != other.incrementBy)
			return false;
		if (maxValue != other.maxValue)
			return false;
		if (minValue != other.minValue)
			return false;
		return true;
	}
	
}
