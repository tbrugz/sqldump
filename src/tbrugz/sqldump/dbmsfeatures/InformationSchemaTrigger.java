package tbrugz.sqldump.dbmsfeatures;

import java.util.Set;
import java.util.TreeSet;

import tbrugz.sqldump.Utils;
import tbrugz.sqldump.dbmodel.Trigger;

public class InformationSchemaTrigger extends Trigger {
	public Set<String> eventsManipulation = new TreeSet<String>();
	public String actionStatement; 
	public String actionOrientation; 
	public String conditionTiming;
	static boolean addSplitter; //XXX: should be static? or belongs to model?
	
	@Override
	public String getDefinition(boolean dumpSchemaName) {
		return "create trigger "+name
				+"\n  "+conditionTiming+" "+Utils.join(eventsManipulation, " or ")
				+"\n  on "+tableName
				+"\n  for each "+actionOrientation
				+"\n  "+actionStatement
				+(addSplitter?";":"");
	}

}
