package tbrugz.sqldump.dbmodel;

import java.util.Comparator;

public interface NamedDBObject /* extends Comparable<NamedDBObject>? */ {

	public String getName();
	
	public String getSchemaName();
	
	//public String getRemarks();
	
	// java8+
	public default String getQualifiedName() {
		return (getSchemaName()!=null?getSchemaName()+".":"")+getName();
	};

	public static class SchemaAndNameComparator implements Comparator<NamedDBObject> {
		@Override
		public int compare(NamedDBObject o1, NamedDBObject o2) {
			int comp = 0;
			if(o1.getSchemaName()!=null) {
				if(o2.getSchemaName()!=null) {
					comp = o1.getSchemaName().compareTo(o2.getSchemaName());
				}
				else {
					comp = 1;
				}
			}
			else {
				if(o2.getSchemaName()!=null) {
					comp = -1;
				}
				//else { comp = 0; }
			}
			if(comp==0) {
				comp = o1.getName().compareTo(o2.getName());
			}
			return comp;
		}
	}

}
