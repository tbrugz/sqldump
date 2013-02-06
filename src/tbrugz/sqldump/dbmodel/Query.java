package tbrugz.sqldump.dbmodel;

import java.util.List;

public class Query extends View {
	private static final long serialVersionUID = 1L;

	public String id;
	public List<String> parameterValues;
	public Integer parameterCount;
}
