package tbrugz.sqldump.dbmsfeatures;

import java.sql.DatabaseMetaData;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import tbrugz.sqldump.dbmd.DefaultDBMSFeatures;

public class MSAccessFeatures extends DefaultDBMSFeatures {
	
	static Map<Class<?>, Class<?>> columnTypeMapper = new HashMap<Class<?>, Class<?>>();
	
	static {
		columnTypeMapper.put(Date.class, String.class);
		columnTypeMapper.put(Integer.class, String.class);
	}
	
	@Override
	public Map<Class<?>, Class<?>> getColumnTypeMapper() {
		return columnTypeMapper;
	}
	
	@Override
	public DatabaseMetaData getMetadataDecorator(DatabaseMetaData metadata) {
		return new MSAccessDatabaseMetaData(metadata);
	}

}
