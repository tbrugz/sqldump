package tbrugz.sqldump.dbmsfeatures;

import java.util.Set;
import java.util.TreeSet;

import tbrugz.sqldump.Utils;
import tbrugz.sqldump.dbmodel.Trigger;

public class InformationSchemaTrigger extends Trigger {
	//String query = "select trigger_catalog, trigger_schema, trigger_name, event_manipulation, event_object_schema, event_object_table, action_statement, action_orientation, condition_timing "
	Set<String> eventsManipulation = new TreeSet<String>();
	String actionStatement; 
	String actionOrientation; 
	String conditionTiming;
	
	@Override
	public String getDefinition(boolean dumpSchemaName) {
		return "create trigger "+name
				+"\n  "+conditionTiming+" "+Utils.join(eventsManipulation, " or ")
				+"\n  on "+tableName
				+"\n  for each "+actionOrientation
				+"\n  "+actionStatement;
	}

}
