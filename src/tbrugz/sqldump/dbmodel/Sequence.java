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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + (int) (incrementBy ^ (incrementBy >>> 32));
		result = prime * result
				+ ((maxValue == null) ? 0 : maxValue.hashCode());
		result = prime * result
				+ ((minValue == null) ? 0 : minValue.hashCode());
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
		Sequence other = (Sequence) obj;
		if (incrementBy != other.incrementBy)
			return false;
		if (maxValue == null) {
			if (other.maxValue != null)
				return false;
		} else if (!maxValue.equals(other.maxValue))
			return false;
		if (minValue == null) {
			if (other.minValue != null)
				return false;
		} else if (!minValue.equals(other.minValue))
			return false;
		return true;
	}
	
}
