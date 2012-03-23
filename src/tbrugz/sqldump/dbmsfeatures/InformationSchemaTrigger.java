package tbrugz.sqldump.dbmsfeatures;

import java.util.Set;
import java.util.TreeSet;

import tbrugz.sqldump.dbmodel.Trigger;
import tbrugz.sqldump.util.Utils;

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
	
	@Override
	public String toString() {
		return "[ISTrigger:"+name+"/"+tableName+":"+conditionTiming+","+actionOrientation+","+eventsManipulation+"]";
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		InformationSchemaTrigger other = (InformationSchemaTrigger) obj;
		if (actionOrientation == null) {
			if (other.actionOrientation != null)
				return false;
		} else if (!actionOrientation.equals(other.actionOrientation))
			return false;
		if (actionStatement == null) {
			if (other.actionStatement != null)
				return false;
		} else if (!actionStatement.equals(other.actionStatement))
			return false;
		if (conditionTiming == null) {
			if (other.conditionTiming != null)
				return false;
		} else if (!conditionTiming.equals(other.conditionTiming))
			return false;
		if (eventsManipulation == null) {
			if (other.eventsManipulation != null)
				return false;
		} else if (!eventsManipulation.equals(other.eventsManipulation))
			return false;
		return true;
	}
}
