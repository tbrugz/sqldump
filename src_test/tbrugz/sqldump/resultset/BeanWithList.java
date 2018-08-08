package tbrugz.sqldump.resultset;

import java.util.Arrays;
import java.util.List;

public class BeanWithList {
	
	final int id;
	final List<String> names;
	
	static String[] uniqueCols = {"id"};
	static String[] allCols = {"names"};
	
	public BeanWithList(int id, List<String> names) {
		this.id = id;
		this.names = names;
	}
	
	public static List<String> getUniqueCols() {
		return Arrays.asList(uniqueCols);
	}

	public static List<String> getAllCols() {
		return Arrays.asList(allCols);
	}
	
	public int getId() {
		return id;
	}
	
	public List<String> getNames() {
		return names;
	}
}
