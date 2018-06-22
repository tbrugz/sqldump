package tbrugz.sqldump;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import tbrugz.sqldiff.CompareTest;
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
import tbrugz.sqldump.datadump.ConcurrentDumpTest;
import tbrugz.sqldump.datadump.DataDumpTest;
import tbrugz.sqldump.datadump.JsonTest;
import tbrugz.sqldump.dbmodel.ColTypeUtilTest;
import tbrugz.sqldump.dbmodel.ColumnTest;
import tbrugz.sqldump.dbmodel.DBObjectUtilsTest;
import tbrugz.sqldump.dbmodel.GrantTest;
import tbrugz.sqldump.dbmsfeatures.FunctionTest;
import tbrugz.sqldump.dbmsfeatures.TriggerTest;
//import tbrugz.sqldump.dbmsfeatures.oracle.OracleTest;
import tbrugz.sqldump.def.DBMSResourcesTest;
import tbrugz.sqldump.graph.R2GTest;
import tbrugz.sqldump.mondrianschema.MondrianTests;
import tbrugz.sqldump.pivot.DriverTest;
import tbrugz.sqldump.pivot.KeyTest;
import tbrugz.sqldump.pivot.QueryTest;
import tbrugz.sqldump.processors.CascadingDataDumpTest;
import tbrugz.sqldump.processors.SQLRunProcessorTest;
import tbrugz.sqldump.resultset.RsProjectionAdapterTest;
import tbrugz.sqldump.resultset.RsListAdapterModelTest;
import tbrugz.sqldump.resultset.pivot.PivotRSTest;
import tbrugz.sqldump.sqlrun.CSVImportTest;
import tbrugz.sqldump.sqlrun.FailoverTest;
import tbrugz.sqldump.sqlrun.SQLRunAndDumpTest;
import tbrugz.sqldump.sqlrun.SQLRunMainTest;
import tbrugz.sqldump.sqlrun.SQLTokenizersTest;
import tbrugz.sqldump.sqlrun.SqlImportTest;
import tbrugz.sqldump.sqlrun.StmtExecTest;
import tbrugz.sqldump.sqlrun.StmtProcTest;
import tbrugz.sqldump.sqlrun.XlsImportTest;
import tbrugz.sqldump.sqlrun.tokenzr.SQLStmtScannerTest;
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
	SQLStmtScannerTest.class,
	PivotRSTest.class,
	IOUtilTest.class,
	CategorizedOutTest.class,
	CLIProcessorTest.class,
	DBMSResourcesTest.class,
	StmtProcTest.class,
	ScriptDumperTest.class,
	//model unit tests
	ColumnTest.class,
	ColTypeUtilTest.class,
	DBObjectUtilsTest.class,
	GrantTest.class,
	//datadump unit tests
	JsonTest.class,
	//resultset unit tests
	RsListAdapterModelTest.class,
	RsProjectionAdapterTest.class,

	//diff "unit" tests
	ColumnDiffTest.class,
	CompareTest.class,
	DiffIOTest.class,
	DiffValidatorTest.class,
	SQLDiffMainTest.class,
	SimilarityCalculatorTest.class,
	RenameDetectorTest.class,
	SchemaDiffTest.class,

	//import
	CSVImportTest.class,
	XlsImportTest.class,
	SqlImportTest.class,
	
	//dbms features
	TriggerTest.class,
	FunctionTest.class,
	
	//run
	FailoverTest.class,
	StmtExecTest.class,
	SQLRunMainTest.class,

	//diff
	DiffFromJAXB.class,
	ResultSetDiffTest.class,
	DiffTwoQueriesTest.class,
	
	//pivot
	DriverTest.class,
	QueryTest.class,
	KeyTest.class,

	//diff + database
	SQLDiffTest.class,
	//DerbyDiffTest.class, //taking too long...
	HSQLDBDiffTest.class,
	DiffApplyTest.class,
	
	//datadump
	DataDumpTest.class,
	CascadingDataDumpTest.class,
	ConcurrentDumpTest.class,
	SQLRunProcessorTest.class,
	
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
	
	//DBMSs
	//OracleTest.class,
})
public class SQLDumpTestSuite {
}
