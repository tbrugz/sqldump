package tbrugz.sqldump.datadump;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;

/*
 * java9: https://docs.oracle.com/javase/9/docs/api/javax/sql/rowset
 */
public class CacheRowSetSyntax extends OutputStreamDumper implements DumpSyntaxBuilder, Cloneable {

	CachedRowSet crs;
	
	@Override
	public void procProperties(Properties prop) {
		// do nothing ??
	}

	@Override
	public String getSyntaxId() {
		return "rowset-ser";
	}
	
	@Override
	public String getDefaultFileExtension() {
		return "rowset.ser";
	}

	/*
	 * https://docs.oracle.com/javase/7/docs/api/java/awt/datatransfer/DataFlavor.html - application/x-java-serialized-object
	 * http://www.freeformatter.com/mime-types-list.html - application/java-serialized-object
	 */
	@Override
	public String getMimeType() {
		return "application/java-serialized-object";
	}

	@Override
	public void initDump(String schemaName, String tableName, List<String> pkCols, ResultSetMetaData md) throws SQLException {
		// do nothing ??
	}

	@Override
	public void dumpHeader(OutputStream os) throws IOException {
		// do nothing
	}

	@Override
	public void dumpRow(ResultSet rs, long count, OutputStream os) throws IOException, SQLException {
		if(count==0) {
			dumpHeaderInternal(rs);
		}
		// will dump on dumpFooter
	}

	@Override
	public void dumpFooter(long count, boolean hasMoreRows, OutputStream os) throws IOException {
		// dumps all ResultSet
		ObjectOutputStream out = new ObjectOutputStream(os);
		out.writeObject(crs);
		out.close();
	}
	
	@Override
	public boolean isStateful() {
		return true;
	}
	
	/**
	 * CacheRowSetSyntax fetches all rows from the resultset itself
	 */
	@Override
	public boolean isFetcherSyntax() {
		return true;
	}
	
	/**
	 * CacheRowSetSyntax cant be used in a partitioned strategy because all rows are written
	 * by the dumpFooter() method.
	 */
	@Override
	public boolean isPartitionable() {
		return false;
	}
	
	@Override
	public boolean acceptsOutputStream() {
		return true;
	}
	
	//-----------------------------
	
	void dumpHeaderInternal(ResultSet rs) throws SQLException {
		RowSetFactory factory = RowSetProvider.newFactory();
		crs = factory.createCachedRowSet();
		crs.populate(rs);
	}

}
