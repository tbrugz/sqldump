package tbrugz.sqldump;

import java.util.ArrayList;
import java.util.List;

public class Table {
	String name;
	TableType type;
	List<Column> columns = new ArrayList<Column>();
	
	Column getColumn(String name) {
		if(name==null) return null;
		for(Column c: columns) {
			if(name.equals(c.name)) return c;
		}
		return null;
	}
	
	@Override
	public String toString() {
		return type+":"+name;
		//return "t:"+name;
		//return "Table[name:"+name+"]";
	}
}
