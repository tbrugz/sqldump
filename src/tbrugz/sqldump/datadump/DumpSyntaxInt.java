package tbrugz.sqldump.datadump;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import tbrugz.sqldump.dbmodel.Constraint;
import tbrugz.sqldump.dbmodel.FK;

public interface DumpSyntaxInt {

	public void procProperties(Properties prop);

	public String getSyntaxId();

	public String getMimeType();
	
	public String getDefaultFileExtension();

	public void initDump(String tableName, List<String> pkCols, ResultSetMetaData md) throws SQLException;

	public void setImportedFKs(List<FK> fks);
	
	public void setAllUKs(List<Constraint> uks);
	
	public void dumpHeader(Writer fos) throws IOException;

	public void dumpRow(ResultSet rs, long count, Writer fos) throws IOException, SQLException;

	public void dumpFooter(long count, Writer fos) throws IOException; //long count or boolean hasDumpedData?
	
	//XXX dumpBreak() - intermetiate "header" dump? endDump()?

	public void flushBuffer(Writer fos) throws IOException;
	
	/**
	 * Should return true if responsable for creating output files
	 * 
	 * BlobDataDump will return true (writes 1 file per table row, partitioning doesn't make sense)
	 */
	public boolean isWriterIndependent();

	/**
	 * Should return true if dumpsyntax has buffer. Stateful dumpers should implement clone()
	 * 
	 * FFCDataDump is stateful
	 */
	public boolean isStateful();

	/**
	 * Should return true if imported FKs are used for datadump
	 */
	public boolean usesImportedFKs();

	/**
	 * Should return true if all Unique Keys are used for datadump
	 */
	public boolean usesAllUKs();
	
	/**
	 * Stateful dumpers should implement {@link java.lang.Cloneable}.
	 * Clone() should ideally only copy spec fields (defined by properties) 
	 */
	// making clone() public instead of protected
	// public DumpSyntaxInt cloneSpec();
	public Object clone() throws CloneNotSupportedException;
}
