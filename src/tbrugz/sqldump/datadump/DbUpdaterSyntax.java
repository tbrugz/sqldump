package tbrugz.sqldump.datadump;

import java.sql.Connection;

/**
 * Interaface for syntaxes that update a database based on a (update) Connection.
 */
public interface DbUpdaterSyntax {
	
	public void setUpdaterConnection(Connection conn);

}
