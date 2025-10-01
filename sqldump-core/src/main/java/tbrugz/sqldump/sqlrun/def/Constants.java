package tbrugz.sqldump.sqlrun.def;

public class Constants {

	public static final String STDIN = "<stdin>";
	
	//prefixes
	public static final String SQLRUN_PROPS_PREFIX = "sqlrun";
	public static final String PREFIX_EXEC = SQLRUN_PROPS_PREFIX + ".exec.";
	public static final String PREFIX_ASSERT = SQLRUN_PROPS_PREFIX + ".assert.";

	//base suffixes
	public static final String SUFFIX_IMPORT = ".import";
	public static final String SUFFIX_FAILONERROR = "failonerror";
	
	//suffixes
	public static final String SUFFIX_BATCH_MODE = "batchmode";
	public static final String SUFFIX_BATCH_SIZE = "batchsize";
	public static final String SUFFIX_BATCH_RETRY_OFF = "batchmode.retry-with-batch-off";
	public static final String SUFFIX_ENCODING = "encoding";
	public static final String SUFFIX_LOG_EACH_X_INPUT_ROWS = "log-each-x-input-rows";
	public static final String SUFFIX_LOG_EACH_X_OUTPUT_ROWS = "log-each-x-output-rows";

	//commom importer suffixes
	public static final String SUFFIX_IMPORTDIR = "importdir";
	public static final String SUFFIX_IMPORTFILES = "importfiles";
	public static final String SUFFIX_IMPORTFILES_GLOB = "importfiles.glob";

	public static final String SUFFIX_1ST_LINE_AS_COLUMN_NAMES = "1st-line-as-column-names";
	public static final String SUFFIX_COLUMN_NAMES = "columnnames";
	public static final String SUFFIX_COLUMN_TYPES = "columntypes";
	public static final String SUFFIX_DO_CREATE_TABLE = "do-create-table";
	public static final String SUFFIX_IMPORTFILE = "importfile";
	public static final String SUFFIX_INSERTTABLE = "inserttable";
	public static final String SUFFIX_INSERTSQL = "insertsql";
	public static final String SUFFIX_STATEMENT_AFTER = "statement-after";
	public static final String SUFFIX_STATEMENT_BEFORE = "statement-before";
	public static final String SUFFIX_SKIP_N = "skipnlines";
	//public static final String SUFFIX_TRUNCATE_BEFORE = "truncate-before";
	public static final String SUFFIX_LIMIT_LINES = "limit";
	public static final String SUFFIX_LIMIT_INPUT = "limit-input";

}
