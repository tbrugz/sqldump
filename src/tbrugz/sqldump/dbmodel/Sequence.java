package tbrugz.sqldump.dbmodel;

/*
 * see: http://download.oracle.com/docs/cd/E14072_01/server.112/e10592/statements_6015.htm
 */
public class Sequence extends DBObject {
	
	public long minValue;
	public long maxValue; //XXX: not used yet
	public long incrementBy;
	public long lastNumber;
	
	public static boolean dumpStartWith = false;

	@Override
	public String getDefinition(boolean dumpSchemaName) {
    	return "create sequence "+(dumpSchemaName?schemaName+".":"")+name
    		+" minvalue "+minValue
    		+(dumpStartWith?" start with "+lastNumber:"")
    		+" increment by "+incrementBy
    		+";";
	}

}
