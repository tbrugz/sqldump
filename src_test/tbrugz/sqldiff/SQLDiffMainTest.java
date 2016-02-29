package tbrugz.sqldiff;

import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import javax.naming.NamingException;
import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;

import org.junit.Assert;
import org.junit.Test;

import tbrugz.sqldiff.io.XMLDiffIO;
import tbrugz.sqldiff.model.ChangeType;
import tbrugz.sqldiff.model.ColumnDiff;
import tbrugz.sqldiff.model.DBIdentifiableDiff;
import tbrugz.sqldiff.model.Diff;
import tbrugz.sqldiff.model.SchemaDiff;
import tbrugz.sqldump.TestUtil;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.util.IOUtil;
import tbrugz.sqldump.util.ParametrizedProperties;

public class SQLDiffMainTest {

	String OUTDIR = "work/output/SQLDiffMainTest";
	
	@Test
	public void testMain1() throws IOException, ClassNotFoundException, SQLException, NamingException, JAXBException, XMLStreamException, InterruptedException, ExecutionException, TimeoutException {
		Properties p = new ParametrizedProperties();
		p.load(SQLDiffMainTest.class.getResourceAsStream("diff1.properties"));
		p.setProperty("outputdir", OUTDIR);
		SQLDiff sqld = new SQLDiff();
		sqld.doMain(TestUtil.NULL_PARAMS, p);
		String sql = IOUtil.readFromFilename(OUTDIR+"/"+"diff-PUBLIC-COLUMN.sql");
		if(sql!=null) { sql = sql.trim(); } 
		Assert.assertEquals("alter table PUBLIC.EMP add column EMAIL VARCHAR(100);", sql);
	}

	@Test
	public void testMain3diff() throws Exception {
		Properties p = new ParametrizedProperties();
		p.load(SQLDiffMainTest.class.getResourceAsStream("diff3.properties"));
		p.setProperty("outputdir", OUTDIR);
		SQLDiff sqld = new SQLDiff();
		sqld.doMain(TestUtil.NULL_PARAMS, p);
		
		XMLDiffIO xio = new XMLDiffIO();
		SchemaDiff sd = (SchemaDiff) xio.grabDiff(new FileReader(OUTDIR+"/"+"diff.xml"));
		Assert.assertEquals(0, sd.getDiffList().size());
	}
	
	@Test
	public void testMain4diff() throws Exception {
		Properties p = new ParametrizedProperties();
		p.load(SQLDiffMainTest.class.getResourceAsStream("diff4.properties"));
		p.setProperty("outputdir", OUTDIR);
		SQLDiff sqld = new SQLDiff();
		sqld.doMain(TestUtil.NULL_PARAMS, p);
		
		XMLDiffIO xio = new XMLDiffIO();
		SchemaDiff sd = (SchemaDiff) xio.grabDiff(new FileReader(OUTDIR+"/"+"diff.xml"));
		Assert.assertEquals(3, sd.getDiffList().size());
		List<Diff> diffs = sd.getChildren();
		int colRename = 0, colAdd = 0, constraintRename = 0;
		for(Diff d: diffs) {
			if(d instanceof ColumnDiff && d.getChangeType()==ChangeType.ADD) { colAdd++; }
			if(d instanceof ColumnDiff && d.getChangeType()==ChangeType.RENAME) { colRename++; }
			if(d instanceof DBIdentifiableDiff && d.getChangeType()==ChangeType.RENAME && d.getObjectType()==DBObjectType.CONSTRAINT) { constraintRename++; }
		}
		Assert.assertEquals(1, colRename);
		Assert.assertEquals(1, colAdd);
		Assert.assertEquals(1, constraintRename);
	}
}
