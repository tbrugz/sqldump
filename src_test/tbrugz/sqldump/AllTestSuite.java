package tbrugz.sqldump;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import tbrugz.sqldiff.DiffTwoQueriesTest;
import tbrugz.sqldiff.SQLDiffMainTest;
import tbrugz.sqldiff.apply.DiffApplyTest;
import tbrugz.sqldiff.datadiff.ResultSetDiffTest;
import tbrugz.sqldiff.io.DiffIOTest;
import tbrugz.sqldiff.model.ColumnDiffTest;
import tbrugz.sqldiff.model.SchemaDiffTest;
import tbrugz.sqldiff.test.DiffFromJAXB;
import tbrugz.sqldiff.test.HSQLDBDiffTest;
import tbrugz.sqldiff.test.SQLDiffTest;
import tbrugz.sqldiff.util.RenameDetectorTest;
import tbrugz.sqldiff.util.SimilarityCalculatorTest;
import tbrugz.sqldiff.validate.DiffValidatorTest;
import tbrugz.sqldump.ant.AntTasksTest;
import tbrugz.sqldump.datadump.DataDumpTest;
import tbrugz.sqldump.dbmodel.ColTypeUtilTest;
import tbrugz.sqldump.dbmsfeatures.TriggerTest;
import tbrugz.sqldump.def.DBMSResourcesTest;
import tbrugz.sqldump.graph.R2GTest;
import tbrugz.sqldump.mondrianschema.MondrianTests;
import tbrugz.sqldump.pivot.DriverTest;
import tbrugz.sqldump.pivot.QueryTest;
import tbrugz.sqldump.processors.CascadingDataDumpTest;
import tbrugz.sqldump.resultset.pivot.PivotRSTest;
import tbrugz.sqldump.sqlrun.CSVImportTest;
import tbrugz.sqldump.sqlrun.FailoverTest;
import tbrugz.sqldump.sqlrun.SQLRunAndDumpTest;
import tbrugz.sqldump.sqlrun.SQLTokenizersTest;
import tbrugz.sqldump.sqlrun.StmtExecTest;
import tbrugz.sqldump.sqlrun.StmtProcTest;
import tbrugz.sqldump.util.CLIProcessorTest;
import tbrugz.sqldump.util.CategorizedOutTest;
import tbrugz.sqldump.util.ConnectionUtilTest;
import tbrugz.sqldump.util.IOUtilTest;
import tbrugz.sqldump.util.ParametrizedPropertiesTest;

@RunWith(Suite.class)
@SuiteClasses({
	//"unit" tests
	ParametrizedPropertiesTest.class,
	SQLTokenizersTest.class,
	ColTypeUtilTest.class,
	PivotRSTest.class,
	IOUtilTest.class,
	CategorizedOutTest.class,
	CLIProcessorTest.class,
	DBMSResourcesTest.class,
	StmtProcTest.class,

	//diff "unit" tests
	ColumnDiffTest.class,
	DiffIOTest.class,
	DiffValidatorTest.class,
	SQLDiffMainTest.class,
	SimilarityCalculatorTest.class,
	RenameDetectorTest.class,
	SchemaDiffTest.class,

	//import
	CSVImportTest.class,
	
	//dbms features
	TriggerTest.class,
	
	//run
	FailoverTest.class,
	StmtExecTest.class,

	//diff
	DiffFromJAXB.class,
	ResultSetDiffTest.class,
	DiffTwoQueriesTest.class,
	
	//pivot
	DriverTest.class,
	QueryTest.class,

	//diff + database
	SQLDiffTest.class,
	//DerbyDiffTest.class, //taking too long...
	HSQLDBDiffTest.class,
	DiffApplyTest.class,
	
	//datadump
	DataDumpTest.class,
	CascadingDataDumpTest.class,
	
	//graph
	R2GTest.class,
	
	//mondrian
	MondrianTests.class,
	
	//ant tasks
	AntTasksTest.class,
	
	//database/connection
	ConnectionUtilTest.class,
	
	//roundtrip
	SQLRunAndDumpTest.class,
	//RoundTripTest.class,
})
public class AllTestSuite {
}
