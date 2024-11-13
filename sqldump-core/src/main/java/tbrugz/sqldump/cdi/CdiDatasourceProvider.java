package tbrugz.sqldump.cdi;

import javax.enterprise.inject.spi.CDI;
import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.util.DataSourceProvider;

/*
 * Experimental...
 */
/*
public class CdiDatasourceProvider implements DataSourceProvider {

	static final Log log = LogFactory.getLog(CdiDatasourceProvider.class);

	@Override
	public DataSource getDataSource(String name) {
		//log.info("getDataSource: name = "+name+" (name ignored)");
		DataSource ds = CDI.current().select(DataSource.class).get();
		if(ds==null) {
			log.warn("getDataSource: DataSource is null [name="+name+"]");
		}
		return ds;
	}

}
*/