package tbrugz.sqldump;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import tbrugz.sqldiff.datadiff.ResultSetDiffTest;
import tbrugz.sqldiff.io.DiffIOTest;
import tbrugz.sqldiff.model.ColumnDiffTest;
import tbrugz.sqldiff.test.DiffFromJAXB;
import tbrugz.sqldiff.test.HSQLDBDiffTest;
import tbrugz.sqldiff.test.SQLDiffTest;
import tbrugz.sqldump.datadump.DataDumpTest;
import tbrugz.sqldump.dbmodel.ColTypeUtilTest;
import tbrugz.sqldump.def.AbstractFailable;
import tbrugz.sqldump.def.DBMSResourcesTest;
import tbrugz.sqldump.graph.R2GTest;
import tbrugz.sqldump.mondrianschema.MondrianTests;
import tbrugz.sqldump.processors.CascadingDataDumpTest;
import tbrugz.sqldump.resultset.pivot.PivotRSTest;
import tbrugz.sqldump.sqlrun.CSVImportTest;
import tbrugz.sqldump.sqlrun.FailoverTest;
import tbrugz.sqldump.sqlrun.SQLRunAndDumpTest;
import tbrugz.sqldump.sqlrun.SQLTokenizersTest;
import tbrugz.sqldump.sqlrun.StmtExecTest;
import tbrugz.sqldump.util.CLIProcessorTest;
import tbrugz.sqldump.util.CategorizedOutTest;
import tbrugz.sqldump.util.IOUtilTest;
import tbrugz.sqldump.util.ParametrizedPropertiesTest;

@RunWith(Suite.class)
@SuiteClasses({
	//"unit" tests
	ParametrizedPropertiesTest.class,
	//SQLTokenizerTest.class,
	SQLTokenizersTest.class,
	ColTypeUtilTest.class,
	PivotRSTest.class,
	IOUtilTest.class,
	CategorizedOutTest.class,
	ColumnDiffTest.class,
	CLIProcessorTest.class,
	DBMSResourcesTest.class,
	DiffIOTest.class,

	//import
	CSVImportTest.class,
	
	//run
	FailoverTest.class,
	StmtExecTest.class,

	//diff
	DiffFromJAXB.class,
	ResultSetDiffTest.class,

	//diff + database
	SQLDiffTest.class,
	//DerbyDiffTest.class, //taking too long...
	HSQLDBDiffTest.class,
	
	//datadump
	DataDumpTest.class,
	CascadingDataDumpTest.class,
	
	//graph
	R2GTest.class,
	
	//mondrian
	MondrianTests.class,
	
	//roundtrip
	SQLRunAndDumpTest.class,
	//RoundTripTest.class,
})
public class AllTestSuite {
	static {
		AbstractFailable.DEFAULT_FAILONERROR = true;
	}
}
