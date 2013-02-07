package tbrugz.sqldiff.compare;

import java.util.Comparator;

import tbrugz.sqldiff.model.Diff;

public class DiffComparator implements Comparator<Diff> {

	@Override
	public int compare(Diff o1, Diff o2) {
		if(o1==null || o2==null) { return 0; }
		//changetype
		int comp = o1.getChangeType().compareTo(o2.getChangeType());
		if(comp!=0) return -comp;
		//difftype
		comp = o1.getObjectType().compareTo(o2.getObjectType());
		//if(comp!=0) return comp;

		return comp;
	}

}
