package tbrugz.sqldump.datadump;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;

public abstract class WriterIndependentDumpSyntax extends AbstractDumpSyntax {

	@Override
	public void dumpHeader(Writer fos) throws IOException {
		throw new IllegalStateException("class "+this.getClass().getSimpleName()+" does not implements this method");
	}
	
	@Override
	public void dumpRow(ResultSet rs, long count, Writer fos) throws IOException, SQLException {
		throw new IllegalStateException("class "+this.getClass().getSimpleName()+" does not implements this method");
	}
	
	@Override
	public void dumpFooter(long count, Writer fos) throws IOException {
		throw new IllegalStateException("class "+this.getClass().getSimpleName()+" does not implements this method");
	}
	
	@Override
	public boolean isWriterIndependent() {
		return true;
	}
}
