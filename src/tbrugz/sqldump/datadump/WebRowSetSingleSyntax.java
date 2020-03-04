package tbrugz.sqldump.datadump;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import javax.sql.rowset.WebRowSet;

import com.sun.rowset.WebRowSetImpl;

/**
 * Should be used isolated (single) from other syntaxes, as it reads all ResultSet
 * in WebRowSetImpl.populate()
 * 
 * see also: http://www.onjava.com/pub/a/onjava/2006/06/21/making-most-of-jdbc-with-webrowset.html?page=3
 * 
 * column BLOB, CLOB, Array, Ref: wsrxmlwriter.notproper - http://j7a.ru/_web_row_set_xml_writer_8java_source.html
 */
/*
 * java9: https://docs.oracle.com/javase/9/docs/api/javax/sql/rowset
 */
public class WebRowSetSingleSyntax extends AbstractDumpSyntax implements DumpSyntaxBuilder, Cloneable {

	WebRowSet wrs;
	
	@Override
	public void procProperties(Properties prop) {
		// do nothing ??
	}

	@Override
	public String getSyntaxId() {
		return "webrowset-single";
	}
	
	@Override
	public String getDefaultFileExtension() {
		return "webrowset.xml";
	}

	@Override
	public String getMimeType() {
		return "application/xml";
	}

	@Override
	public void initDump(String schemaName, String tableName,
			List<String> pkCols, ResultSetMetaData md) throws SQLException {
		// do nothing ??
	}

	@Override
	public void dumpHeader(Writer fos) throws IOException {
		// do nothing
	}

	@Override
	public void dumpRow(ResultSet rs, long count, Writer fos)
			throws IOException, SQLException {
		if(count==0) {
			dumpHeaderInternal(rs);
		}
		// will dump on dumpFooter
	}

	@Override
	public void dumpFooter(long count, boolean hasMoreRows, Writer fos) throws IOException {
		try {
			// dumps all ResultSet
			wrs.writeXml(fos);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public boolean isStateful() {
		return true;
	}
	
	/**
	 * WebRowSetSingleSyntax fetches all rows from the resultset itself
	 */
	@Override
	public boolean isFetcherSyntax() {
		return true;
	}
	
	/**
	 * WebRowSetSingleSyntax cant be used in a partitioned strategy because all rows are written
	 * by the dumpFooter() method.
	 */
	@Override
	public boolean isPartitionable() {
		return false;
	}
	
	void dumpHeaderInternal(ResultSet rs) throws SQLException {
		wrs = new WebRowSetImpl();
		wrs.populate(rs);
	}

}
