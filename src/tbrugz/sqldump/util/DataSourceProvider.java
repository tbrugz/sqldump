package tbrugz.sqldump.util;

import javax.sql.DataSource;

public interface DataSourceProvider {

	public DataSource getDataSource(String name);
	
}
