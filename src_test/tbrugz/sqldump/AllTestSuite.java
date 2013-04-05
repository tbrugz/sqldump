package tbrugz.sqldump;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import tbrugz.sqldiff.datadiff.ResultSetDiffTest;
import tbrugz.sqldiff.test.DiffFromJAXB;
import tbrugz.sqldiff.test.HSQLDBDiffTest;
import tbrugz.sqldiff.test.SQLDiffTest;
import tbrugz.sqldump.datadump.DataDumpTest;
import tbrugz.sqldump.dbmodel.ColTypeUtilTest;
import tbrugz.sqldump.def.AbstractFailable;
import tbrugz.sqldump.resultset.pivot.PivotRSTest;
import tbrugz.sqldump.sqlrun.CSVImportTest;
import tbrugz.sqldump.sqlrun.FailoverTest;
import tbrugz.sqldump.sqlrun.SQLRunAndDumpTest;
import tbrugz.sqldump.test.SQLTokenizerTest;
import tbrugz.sqldump.util.CategorizedOutTest;
import tbrugz.sqldump.util.IOUtilTest;
import tbrugz.sqldump.util.ParametrizedPropertiesTest;

@RunWith(Suite.class)
@SuiteClasses({
	//"unit" tests
	DataDumpTest.class,
	ParametrizedPropertiesTest.class,
	SQLTokenizerTest.class,
	ColTypeUtilTest.class,
	PivotRSTest.class,
	IOUtilTest.class,
	CategorizedOutTest.class,

	//import
	CSVImportTest.class,
	
	//run
	FailoverTest.class,

	//dump ?
	//SQLDumpTestSuite.class,
	
	//diff
	DiffFromJAXB.class,
	ResultSetDiffTest.class,

	//diff + database
	SQLDiffTest.class,
	//DerbyDiffTest.class, //taking too long...
	HSQLDBDiffTest.class,
	
	//roundtrip
	SQLRunAndDumpTest.class,
	//RoundTripTest.class,
})
public class AllTestSuite {
	static {
		AbstractFailable.DEFAULT_FAILONERROR = true;
	}
}
