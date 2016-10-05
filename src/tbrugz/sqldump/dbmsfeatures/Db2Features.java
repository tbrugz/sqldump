package tbrugz.sqldump.dbmsfeatures;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.Sequence;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.dbmodel.Trigger;

public class Db2Features extends InformationSchemaFeatures {
	static Log log = LogFactory.getLog(Db2Features.class);
	
	public Db2Features() {
		setInformationSchemaName("sysibm");
	}
	
	/*
	 * select * from syscat.triggers
	 */
	@Override
	public void grabDBTriggers(Collection<Trigger> triggers,
			String schemaPattern, String tableNamePattern,
			String triggerNamePattern, Connection conn) throws SQLException {
		log.warn("grabDBTriggers: not implemented");
	}
	
	/*
	 * select * from syscat.sequences
	 */
	@Override
	public void grabDBSequences(Collection<Sequence> seqs, String schemaPattern,
			String sequenceNamePattern, Connection conn) throws SQLException {
		log.warn("grabDBSequences: not implemented");
	}
	
	/*
	 * select * from sysibm.check_constraints
	 * ?? select * from ?.constraint_column_usage
	 */
	@Override
	public void grabDBCheckConstraints(Collection<Table> tables,
			String schemaPattern, String constraintNamePattern, Connection conn)
			throws SQLException {
		log.warn("grabDBCheckConstraints: not implemented");
	}

	/*
	 * select * from sysibm.table_constraints
	 * ?? select * from ?.constraint_column_usage
	 */
	@Override
	public void grabDBUniqueConstraints(Collection<Table> tables,
			String schemaPattern, String constraintNamePattern, Connection conn)
			throws SQLException {
		log.warn("grabDBUniqueConstraints: not implemented");
	}

}
