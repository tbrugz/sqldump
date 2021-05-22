package tbrugz.sqldump.sqlrun.def;

public class Constants {

	public static final String STDIN = "<stdin>";
	
	//prefixes
	public static final String SQLRUN_PROPS_PREFIX = "sqlrun";
	public static final String PREFIX_EXEC = SQLRUN_PROPS_PREFIX + ".exec.";
	public static final String PREFIX_ASSERT = SQLRUN_PROPS_PREFIX + ".assert.";

	//suffixes
	public static final String SUFFIX_IMPORT = ".import";
	public static final String SUFFIX_BATCH_MODE = ".batchmode";
	public static final String SUFFIX_BATCH_SIZE = ".batchsize";
	public static final String SUFFIX_BATCH_RETRY_OFF = ".batchmode.retry-with-batch-off";
	public static final String SUFFIX_ENCODING = ".encoding";
	public static final String SUFFIX_DEFAULT_ENCODING = ".defaultencoding";
	public static final String SUFFIX_FAILONERROR = ".failonerror";
	public static final String SUFFIX_LOG_EACH_X_INPUT_ROWS = ".log-each-x-input-rows";
	public static final String SUFFIX_LOG_EACH_X_OUTPUT_ROWS = ".log-each-x-output-rows";

	//commom importer suffixes
	public static final String SUFFIX_COLUMN_TYPES = ".columntypes";
	public static final String SUFFIX_IMPORTFILE = ".importfile";
	public static final String SUFFIX_INSERTTABLE = ".inserttable";
	public static final String SUFFIX_INSERTSQL = ".insertsql";
	public static final String SUFFIX_SKIP_N = ".skipnlines";

}
