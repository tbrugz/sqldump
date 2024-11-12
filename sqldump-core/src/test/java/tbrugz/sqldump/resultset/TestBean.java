package tbrugz.sqldump.resultset;

import java.util.Arrays;
import java.util.List;

public class TestBean {
	final int id;
	final String description;
	final String category;
	final int measure;
	
	static String[] uniqueCols = {"id"};
	static String[] allCols = {"description", "category", "measure"};
	
	public TestBean(int id, String desc, String cat, int measure) {
		this.id = id;
		this.description = desc;
		this.category = cat;
		this.measure = measure;
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

	public String getDescription() {
		return description;
	}

	public String getCategory() {
		return category;
	}

	public int getMeasure() {
		return measure;
	}
	
}
