package tbrugz.sqlmigrate;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldiff.model.SchemaDiff;
import tbrugz.sqldump.datadump.DataDumpUtils;
import tbrugz.sqldump.sqlrun.def.Importer;
import tbrugz.sqldump.sqlrun.tokenzr.Tokenizer;
import tbrugz.sqldump.sqlrun.tokenzr.TokenizerStrategy;
import tbrugz.sqldump.sqlrun.tokenzr.TokenizerUtil;
import tbrugz.sqldump.util.ConnectionUtil;
import tbrugz.sqldump.util.Utils;
import tbrugz.sqlmigrate.util.ImporterUtils;
import tbrugz.sqlmigrate.util.SchemaUtils;

public class SqlMigrate extends BaseExecutor {

	static final Log log = LogFactory.getLog(SqlMigrate.class);

	static final String PROPERTIES_FILENAME = "sqlmigrate.properties";
	//static final String CONN_PROPS_PREFIX = "sqlmigrate";
	static final String PRODUCT_NAME = "sqlmigrate";

	static final String PROP_ACTION = PRODUCT_NAME + "." + "action";
	static final String PROP_MIGRATION_TABLE = PRODUCT_NAME + "." + "migration-table";
	static final String PROP_MIGRATION_TABLE_SCHEMA = PRODUCT_NAME + "." + "schema-name";
	static final String PROP_MIGRATIONS_DIR = PRODUCT_NAME + "." + "migrations-dir";
	static final String PROP_REPEATABLE_MIGRATIONS_DIR = PRODUCT_NAME + "." + "repeatable-migrations-dir";
	static final String PROP_BASELINE = PRODUCT_NAME + "." + "baseline";
	static final String PROP_BASELINE_VERSION = PRODUCT_NAME + "." + "baseline-version";
	static final String PROP_CHARSET = PRODUCT_NAME + "." + "scripts-charset";

	static final String DEFAULT_MIGRATION_TABLE = "SQLMIGRATE_HISTORY"; //"sqlmigrate_history"; //"sqlm_changelog";
	static final String DEFAULT_MIGRATIONS_DIR = ".";
	static final String DEFAULT_REPEATABLE_MIGRATIONS_DIR = null;
	
	public enum MigrateAction {
		SETUP,
		STATUS,
		MIGRATE,
	}

	static final boolean getChecksumForVersionedScripts = false;
	
	// common properties
	MigrateAction action = null;
	String migrationsSchemaName = null;
	String migrationsTable = DEFAULT_MIGRATION_TABLE;
	String versionedMigrationsDir = null;
	String repeatableMigrationsDir = null;
	String charset = DataDumpUtils.CHARSET_UTF8;
	
	// setup/baseline properties
	boolean baseline = false;
	String baselineVersion = null;

	@Override
	public Log getLogger() {
		return log;
	}

	@Override
	public String getProductName() {
		return PRODUCT_NAME;
	}

	@Override
	public String getPropertiesFilename() {
		return PROPERTIES_FILENAME;
	}

	@Override
	public String getPropertiesPrefix() {
		return PRODUCT_NAME;
	}

	//@Override
	//protected void init(Connection c) {
	//}
	
	/*
	@Override
	protected String[] preprocessArgs(String[] args) {
		// if args[i]==action, then set action, remove arg from array
		return super.preprocessArgs(args);
	}
	*/
	
	@Override
	protected void postProcessArgs(List<String> xtraArgs) {
		for(String arg: xtraArgs) {
			if(action!=null) {
				throw new IllegalArgumentException("Illegal argument '"+arg+"'. Action '"+action+"' already set");
			}
			try {
				action = MigrateAction.valueOf(arg.toUpperCase());
			}
			catch(IllegalArgumentException e) {
				throw new IllegalArgumentException("unknown action '"+arg+"'", e);
			}
		}
		/*if(xtraArgs.size()>0) {
			CLIProcessor.throwUnknownArgException(xtraArgs.get(0));
		}*/
	}
	
	@Override
	public void procProterties() {
		String actionStr = papp.getProperty(PROP_ACTION);
		if(actionStr!=null) {
			if(action==null) {
				try {
					action = MigrateAction.valueOf(actionStr.toUpperCase());
				}
				catch(IllegalArgumentException e) {
					throw new IllegalArgumentException("unknown action '"+actionStr+"'", e);
				}
			}
			else {
				throw new IllegalArgumentException("action '"+action+"' already set [prop '"+PROP_ACTION+"' = '"+actionStr+"']");
			}
		}
		migrationsTable = papp.getProperty(PROP_MIGRATION_TABLE, DEFAULT_MIGRATION_TABLE);
		migrationsSchemaName = papp.getProperty(PROP_MIGRATION_TABLE_SCHEMA);
		versionedMigrationsDir = papp.getProperty(PROP_MIGRATIONS_DIR, DEFAULT_MIGRATIONS_DIR);
		repeatableMigrationsDir = papp.getProperty(PROP_REPEATABLE_MIGRATIONS_DIR, DEFAULT_REPEATABLE_MIGRATIONS_DIR);
		baseline = Utils.getPropBool(papp, PROP_BASELINE, baseline);
		baselineVersion = papp.getProperty(PROP_BASELINE_VERSION, baselineVersion);
		charset = papp.getProperty(PROP_CHARSET, charset);
	}
	
	@Override
	protected void initCheck() {
		if(action==null) {
			throw new IllegalStateException("action not set");
		}
	}

	public static void main(String[] args) throws ClassNotFoundException, IOException, SQLException, NamingException, IllegalStateException {
		SqlMigrate sqlm = new SqlMigrate();
		sqlm.doMain(args, null, null);
	}
	
	@Override
	protected void doIt() {
		try {
			switch (action) {
			case SETUP:
				doSetup();
				break;
			case STATUS:
				doStatus();
				break;
			case MIGRATE:
				doMigrate();
				break;
			default:
				break;
			}
			//quietRollback();
		} catch (IOException | SQLException e) {
			log.error("doIt: Error: "+e, e);
			throw new RuntimeException(e);
		}
	}
	
	/*void quietRollback() {
		try {
			if(conn.getAutoCommit()) { return; }
			conn.rollback();
		} catch (SQLException e) {
			log.warn("error in rollback(): "+e, e);
		}
	}*/
	
	/*
	 * setup: create (if needed) the migration table
	 * only changes migration-table
	 * populate/baseline: populate migration table with scripts from migration dir as if they had already executed (optional param: baseline-version)
	 * dry-run: show if table would be created ; [populate/baseline] show scripts that would be populated 
	 */
	public void doSetup() throws IOException, SQLException {
		log.info(">> setup action [dry-run="+isDryRun()+"]");
		SchemaDiff diff = SchemaUtils.diffModel(conn, migrationsSchemaName, migrationsTable);
		if(diff.getDiffListSize()>0) {
			log.info("diffs found [#"+diff.getDiffListSize()+"]");
			SchemaDiff.logInfo(diff);
			SchemaUtils.applySchemaDiff(diff, conn, !isDryRun());
		}
		else {
			log.info("no diffs to apply [diff.getDiffListSize() = "+diff.getDiffListSize()+"]");
		}
		if(baseline) {
			if(diff.getDiffListSize()>0 && isDryRun()) {
				log.warn("diffs found [diff.getDiffListSize() = "+diff.getDiffListSize()+"] & dry-run active (diffs not applyed): can't baseline");
			}
			else {
				doSetupBaseline();
			}
		}
		ConnectionUtil.doRollbackIfNotAutocommit(conn);
	}
	
	void doSetupBaseline() throws SQLException, FileNotFoundException, IOException {
		log.info(">> setup::baseline [dry-run="+isDryRun()+"]");
		long initTime = System.currentTimeMillis();
		boolean removeMigsNotOnFs = true;
		DotVersion upperVersion = null;
		if(baselineVersion!=null) {
			upperVersion = DotVersion.getDotVersion(baselineVersion);
			log.info("baseline-version = "+upperVersion);
		}
		
		MigrationDao md = new MigrationDao(migrationsSchemaName, migrationsTable);
		List<Migration> mds = md.listVersionedMigrations(conn);
		MigrationIO.sortMigrationsByVersion(mds);

		log.info("grabbing fs migrations from: "+versionedMigrationsDir);
		List<Migration> mios = MigrationIO.listMigrations(new File(versionedMigrationsDir), true, getChecksumForVersionedScripts);
		MigrationIO.sortMigrationsByVersion(mios);

		boolean hasDbDuplicates = MigrationIO.hasDuplicatedVersions(mds);
		boolean hasFsDuplicates = MigrationIO.hasDuplicatedVersions(mios);
		if(hasDbDuplicates || hasFsDuplicates) {
			String message = "duplicated migration versions found "+
					"[hasDbDuplicates="+hasDbDuplicates+";hasFsDuplicates="+hasFsDuplicates+"]. can't proceed";
			log.error(message);
			if(failonerror) {
				throw new IllegalStateException(message);
			}
			return;
		}
		
		Set<DotVersion> vdb = MigrationIO.getVersionSet(mds);
		long countSaves = 0, countRemoves = 0, countUnchanged = 0, countIgnored = 0;
		for(Migration m: mios) {
			DotVersion fsmv = m.getVersion();
			if(upperVersion!=null && upperVersion.compareTo(fsmv) < 0) {
				if(vdb.contains(fsmv)) {
					if(!isDryRun()) {
						log.info("version '"+fsmv+"' greater than baseline-version '"+upperVersion+"' and in database. removing");
						md.removeByVersion(m, conn);
						//log.info("-- removed? "+removed);
					}
					else {
						log.info("version '"+fsmv+"' greater than baseline-version '"+upperVersion+"' and in database. would be removed");
					}
					countRemoves += 1;
				}
				else {
					log.info("version '"+fsmv+"' greater than baseline-version '"+upperVersion+"'");
					countIgnored += 1;
				}
			}
			else if(vdb.contains(fsmv)) {
				// db already has this migration, do nothing
				log.debug("migration "+m+" already in database. doing nothing");
				countUnchanged += 1;
			}
			else {
				if(!isDryRun()) {
					// insert/save migration
					log.info("migration "+m+" will be inserted");
					md.save(m, conn);
				}
				else {
					log.info("migration "+m+" would be inserted [dry-run is true]");
				}
				countSaves += 1;
			}
		}
		
		// show (and remove) executed migrations with no filesystem correspondence
		long countRemoveNotOnFs = 0;
		Set<DotVersion> vio = MigrationIO.getVersionSet(mios);
		Set<DotVersion> vdb0 = MigrationIO.diffSets(vdb, vio);
		if(vdb0.size()>0) {
			log.warn(vdb0.size()+" executed migrations with no filesystem correspondence ["+vdb0+"]");
			if(removeMigsNotOnFs) {
				for(DotVersion v: vdb0) {
					Migration m = MigrationIO.getMigrationsFromVersion(mds, v).get(0);
					if(!isDryRun()) {
						log.debug("removing migration "+m);
						md.removeByVersion(m, conn);
					}
					else {
						log.debug("would remove migration "+m);
					}
					countRemoveNotOnFs++;
				}
			}
			log.info("removed "+countRemoveNotOnFs+" executed migrations with no filesystem correspondence");
		}
		
		if(!isDryRun() && (countSaves > 0 || countRemoves > 0 || countRemoveNotOnFs > 0)) {
			ConnectionUtil.doCommitIfNotAutocommit(conn);
		}
		/*else {
			ConnectionUtil.doRollbackIfNotAutocommit(conn);
		}*/
		log.info("baseline: migrations... #saved = "+countSaves +
				" ; #removed = "+countRemoves +
				" ; #unchanged = "+countUnchanged +
				" ; #ignored = "+countIgnored +
				" [elapsed = "+(System.currentTimeMillis()-initTime)+"ms]");
	}
	
	/*
	 * show if migration table is present
	 * show executed & pending migrations
	 * does not perform any change in database (dry-run has no effect)
	 */
	public void doStatus() throws SQLException, FileNotFoundException, IOException {
		log.info(">> status action");
		
		// check if table exists, show diff (type) to be applyed
		SchemaDiff diff = SchemaUtils.diffModel(conn, migrationsSchemaName, migrationsTable);
		if(diff.getDiffListSize()>0) {
			log.info("migration table '"+(migrationsSchemaName!=null?migrationsSchemaName+".":"")+
					migrationsTable+"' needs changes [diff: "+diff+"]");
			log.info("changes: "+diff.getChildren());
			return;
		}
		else {
			log.info("migration table '"+(migrationsSchemaName!=null?migrationsSchemaName+".":"")+
					migrationsTable+"' is up-to-date");
		}
		
		// show migrations
		MigrationDao md = new MigrationDao(migrationsSchemaName, migrationsTable);
		List<Migration> mds = md.listVersionedMigrations(conn);
		MigrationIO.sortMigrationsByVersion(mds);
		if(log.isDebugEnabled()) {
			log.debug(">> db migrations:");
			for(Migration m: mds) {
				log.debug(m);
			}
		}
		
		log.info("grabbing fs migrations from: "+versionedMigrationsDir);
		List<Migration> mios = MigrationIO.listMigrations(new File(versionedMigrationsDir), true, getChecksumForVersionedScripts);
		MigrationIO.sortMigrationsByVersion(mios);
		if(log.isDebugEnabled()) {
			log.debug(">> filesystem migrations:");
			for(Migration m: mios) {
				log.debug(m);
			}
		}
		
		// if has duplications, show duplicates; migrations cannot run when there are dups
		boolean hasDbDuplicates = MigrationIO.hasDuplicatedVersions(mds);
		if(hasDbDuplicates) {
			Set<DotVersion> dups = MigrationIO.getDuplicatedVersions(mds);
			log.warn("database migrations have duplicates [#"+dups.size()+"]");
			log.info("database duplicates: "+dups);
		}
		boolean hasFsDuplicates = MigrationIO.hasDuplicatedVersions(mios);
		if(hasFsDuplicates) {
			Set<DotVersion> dups = MigrationIO.getDuplicatedVersions(mios);
			log.warn("filesystem migrations have duplicates [#"+dups.size()+"]");
			log.info("filesystem duplicates: "+dups);
			for(DotVersion v: dups) {
				List<Migration> dm = MigrationIO.getMigrationsFromVersion(mios, v);
				log.info("version '"+v+"' - dups[#"+dm.size()+"]: "+dm);
			}
		}
		if(hasDbDuplicates || hasFsDuplicates) {
			log.warn("duplicated migration versions found "+
					"[hasDbDuplicates="+hasDbDuplicates+";hasFsDuplicates="+hasFsDuplicates+"]. can't proceed");
			return;
		}
		
		// migrations status
		Set<DotVersion> vdb = MigrationIO.getVersionSet(mds);
		Set<DotVersion> vio = MigrationIO.getVersionSet(mios);

		// show executed migrations with no filesystem correspondence
		Set<DotVersion> vdb0 = MigrationIO.diffSets(vdb, vio);
		if(vdb0.size()>0) {
			log.warn(vdb0.size()+" executed migrations with no filesystem correspondence ["+vdb0+"]");
		}
		
		// show pending migrations
		Set<DotVersion> vio0 = MigrationIO.diffSets(vio, vdb);
		if(vio0.size()>0) {
			log.info(vio0.size()+" pending migrations: "+vio0);
		}
		Set<DotVersion> intersect = MigrationIO.intersectSets(vio, vdb);
		if(intersect.size()>0) {
			log.info(intersect.size()+" migrations already executed: "+intersect);
		}
		
		// show repeatable migrations
		if(repeatableMigrationsDir!=null) {
			doStatus4RepeatableMigrations(md);
		}
		
		ConnectionUtil.doRollbackIfNotAutocommit(conn);
	}
	
	void doStatus4RepeatableMigrations(MigrationDao md) throws SQLException, FileNotFoundException, IOException {
		List<Migration> umd = md.listUnversionedMigrations(conn);
		umd.sort(new Migration.MigrationScriptComparator());
		if(log.isDebugEnabled()) {
			log.debug(">> db repeatable migrations:");
			for(Migration m: umd) {
				log.debug(m);
			}
		}
		log.info("grabbing fs repeatable migrations from: "+repeatableMigrationsDir);
		List<Migration> umfs = MigrationIO.listMigrations(new File(repeatableMigrationsDir), false, true);
		umfs.sort(new Migration.MigrationScriptComparator());
		if(log.isDebugEnabled()) {
			log.debug(">> filesystem repeatable migrations:");
			for(Migration m: umfs) {
				log.debug(m);
			}
		}
		//XXX: check for duplicates... hasDuplicates?
		Set<String> sdb = MigrationIO.getScriptSet(umd);
		Set<String> sio = MigrationIO.getScriptSet(umfs);

		// show executed repeatable migrations with no filesystem correspondence
		Set<String> vdb0 = MigrationIO.diffSets(sdb, sio);
		if(vdb0.size()>0) {
			log.warn(vdb0.size()+" executed repeatable migrations with no filesystem correspondence ["+vdb0+"]");
		}
		
		// show pending migrations
		Set<String> pendingMigs = MigrationIO.diffSets(sio, sdb);
		if(pendingMigs.size()>0) {
			log.info(pendingMigs.size()+" pending repeatable migrations: "+pendingMigs);
		}
		
		// show pending (updated) migrations
		Set<String> intersect = MigrationIO.diffSets(sdb, sio);
		Set<String> unmatchedChecksumMigs = new TreeSet<>();
		for(String s: intersect) {
			List<Migration> mdbl = MigrationIO.getMigrationsFromScript(umd, s);
			List<Migration> mfsl = MigrationIO.getMigrationsFromScript(umfs, s);
			if(mdbl.size()!=1) {
				log.warn("multiple or none migration in DB with script '"+s+"' [#"+mdbl.size()+"]");
				continue;
			}
			if(mfsl.size()!=1) {
				log.warn("multiple or none migration in filesystem with script '"+s+"' [#"+mfsl.size()+"]");
				continue;
			}
			Migration mdb0 = mdbl.get(0);
			Migration mfs0 = mfsl.get(0);
			if(!mdb0.matchesChecksum(mfs0)) {
				unmatchedChecksumMigs.add(s);
				log.debug("script '"+s+"' has unmatching checksum: db="+mdb0.getChecksumString()+" ; fs="+mfs0.getChecksumString());
			}
		}
		if(unmatchedChecksumMigs.size()>0) {
			log.info(unmatchedChecksumMigs.size()+" pending (updated) repeatable migrations: "+unmatchedChecksumMigs);
		}
	}
	
	/*
	 * migrate: execute pending migrations + update migration table
	 * dry-run: only shows with migrations would be executed
	 * options: run-versioned-migrations ; run-repeatable-migrations
	 */
	public void doMigrate() throws SQLException, FileNotFoundException, IOException {
		log.info(">> migrate [dry-run="+isDryRun()+"]");
		long initTime = System.currentTimeMillis();
		if(baseline || baselineVersion!=null) {
			String message = "baseline properties sould not be used in migrate action "+
					"[baseline="+baseline+";baselineVersion="+baselineVersion+"]. can't proceed";
			log.error(message);
			if(failonerror) {
				throw new IllegalArgumentException(message);
			}
			return;
		}
		boolean isAutoCommit = conn.getAutoCommit();
		if(isAutoCommit) { // && !isDryRun() 
			log.warn("autocommit is enabled. not recomended for migrate action");
		}
		/*DotVersion upperVersion = null;
		if(maxVersion!=null) {
			upperVersion = DotVersion.getDotVersion(maxVersion);
			log.info("max-version = "+upperVersion);
		}*/
		
		MigrationDao md = new MigrationDao(migrationsSchemaName, migrationsTable);
		List<Migration> mds = md.listVersionedMigrations(conn);
		MigrationIO.sortMigrationsByVersion(mds);

		log.info("grabbing fs migrations from: "+versionedMigrationsDir);
		List<Migration> mios = MigrationIO.listMigrations(new File(versionedMigrationsDir), true, getChecksumForVersionedScripts);
		MigrationIO.sortMigrationsByVersion(mios);

		boolean hasDbDuplicates = MigrationIO.hasDuplicatedVersions(mds);
		boolean hasFsDuplicates = MigrationIO.hasDuplicatedVersions(mios);
		if(hasDbDuplicates || hasFsDuplicates) {
			String message = "duplicated migration versions found "+
					"[hasDbDuplicates="+hasDbDuplicates+";hasFsDuplicates="+hasFsDuplicates+"]. can't proceed";
			log.error(message);
			if(failonerror) {
				throw new IllegalStateException(message);
			}
			return;
		}
		
		Set<DotVersion> vdb = MigrationIO.getVersionSet(mds);
		long countExecuted = 0, countNotRun = 0;
		try {
			for(Migration m: mios) {
				DotVersion fsmv = m.getVersion();
				if(vdb.contains(fsmv)) {
					// db already has this migration, do nothing
					log.debug("migration "+m+" already executed. doing nothing");
					countNotRun += 1;
				}
				else {
					if(!isDryRun()) {
						// insert/save migration
						log.info("migration "+m+" will be executed");
						// exec and save and commit
						try {
							executeScript(versionedMigrationsDir, m.getScript());
							md.save(m, conn);
							ConnectionUtil.doCommitIfNotAutocommit(conn);
						}
						catch(SQLException e) {
							log.warn("error executing migration: "+e);
							ConnectionUtil.doRollbackIfNotAutocommit(conn);
							if(failonerror) {
								throw e;
							}
						}
					}
					else {
						log.info("migration "+m+" would be executed [dry-run is true]");
					}
					countExecuted += 1;
				}
			}
		}
		finally {
			log.info("migrate: migrations... #executed = "+countExecuted +
					" ; #not-run (already executed) = "+countNotRun +
					" [elapsed = "+(System.currentTimeMillis()-initTime)+"ms]");
		}
		
		// run repeatable migrations
		if(repeatableMigrationsDir!=null) {
			doMigrateRepeatableMigrations(md);
		}

		ConnectionUtil.doRollbackIfNotAutocommit(conn);
	}

	void doMigrateRepeatableMigrations(MigrationDao md) throws SQLException, FileNotFoundException, IOException {
		log.info(">> migrate::repeatables [dry-run="+isDryRun()+"]");
		long initTime = System.currentTimeMillis();
		List<Migration> umd = md.listUnversionedMigrations(conn);
		MigrationIO.sortMigrationsByScript(umd);
		log.info("grabbing fs repeatable migrations from: "+repeatableMigrationsDir);
		List<Migration> umfs = MigrationIO.listMigrations(new File(repeatableMigrationsDir), false, true);
		MigrationIO.sortMigrationsByScript(umfs);
		//XXX: check for duplicates... hasDuplicates?
		Set<String> sdb = MigrationIO.getScriptSet(umd);
		Set<String> sio = MigrationIO.getScriptSet(umfs);

		// show executed repeatable migrations with no filesystem correspondence
		Set<String> vdb0 = MigrationIO.diffSets(sdb, sio);
		if(vdb0.size()>0) {
			String message = vdb0.size()+" executed repeatable migrations with no filesystem correspondence";
			log.warn(message);
			if(failonerror) {
				throw new IllegalStateException(message);
			}
		}
		
		// show pending migrations
		Set<String> pendingMigs = MigrationIO.diffSets(sio, sdb);
		if(pendingMigs.size()>0) {
			log.info(pendingMigs.size()+" pending repeatable migrations: "+pendingMigs);
		}
		
		// show pending (updated) migrations
		//log.debug("db repeatables: "+sdb);
		//log.debug("fs repeatables: "+sio);
		Set<String> intersect = MigrationIO.intersectSets(sdb, sio);
		log.debug("known repeatables [#"+intersect.size()+"]: "+intersect);
		Set<String> unmatchedChecksumMigs = new TreeSet<>();
		long countAlreadyExecuted = 0;
		for(String s: intersect) {
			List<Migration> mdbl = MigrationIO.getMigrationsFromScript(umd, s);
			List<Migration> mfsl = MigrationIO.getMigrationsFromScript(umfs, s);
			if(mdbl.size()!=1) {
				String message = "multiple or none migration in DB with script '"+s+"' [#"+mdbl.size()+"]";
				log.warn(message);
				if(failonerror) {
					throw new IllegalStateException(message);
				}
				continue;
			}
			if(mfsl.size()!=1) {
				String message = "multiple or none migration in filesystem with script '"+s+"' [#"+mfsl.size()+"]";
				log.warn(message);
				if(failonerror) {
					throw new IllegalStateException(message);
				}
				continue;
			}
			Migration mdb0 = mdbl.get(0);
			Migration mfs0 = mfsl.get(0);
			//log.debug("checking for changed checksum: "+s+" "+mdb0.getCrc32()+" / "+mfs0.getCrc32());
			if(!mdb0.matchesChecksum(mfs0)) {
				unmatchedChecksumMigs.add(s);
				log.debug("script '"+s+"' has unmatching checksum: db="+mdb0.getChecksumString()+" ; fs="+mfs0.getChecksumString());
			}
			else {
				countAlreadyExecuted++;
			}
		}
		if(unmatchedChecksumMigs.size()>0) {
			log.info(unmatchedChecksumMigs.size()+" pending (updated) repeatable migrations: "+unmatchedChecksumMigs);
		}
		
		// execute all pending migrations
		Set<String> allPendingMigs = new TreeSet<>();
		allPendingMigs.addAll(pendingMigs);
		allPendingMigs.addAll(unmatchedChecksumMigs);
		long countExecutedNew = 0, countExecutedUpdated = 0;
		try {
			for(String s: allPendingMigs) {
				List<Migration> mdbl = MigrationIO.getMigrationsFromScript(umd, s);
				List<Migration> mfsl = MigrationIO.getMigrationsFromScript(umfs, s);
				Migration mdb = null;
				if(mdbl.size()>0) {
					mdb = mdbl.get(0);
				}
				Migration mfs = mfsl.get(0);
				
				// execute migration
				if(!isDryRun()) {
					// insert/save migration
					log.info( (mdb==null?"new ":"updated ") + "repeatable migration "+mfs+" will be executed");
					// exec and save and commit
					try {
						executeScript(repeatableMigrationsDir, mfs.getScript());
						if(mdb==null) {
							md.save(mfs, conn);
							countExecutedNew++;
						}
						else {
							md.updateChecksumByScript(mdb, mfs.getCrc32(), conn);
							countExecutedUpdated++;
						}
						ConnectionUtil.doCommitIfNotAutocommit(conn);
					}
					catch(SQLException e) {
						log.warn("error executing migration: "+e);
						ConnectionUtil.doRollbackIfNotAutocommit(conn);
						if(failonerror) {
							throw e;
						}
					}
				}
				else {
					log.info( (mdb==null?"new ":"updated ") + "migration "+mfs+" would be executed [dry-run is true]");
					if(mdb==null) {
						countExecutedNew++;
					}
					else {
						countExecutedUpdated++;
					}
				}
			}
		}
		finally {
			log.info("migrate: repeatable migrations... #executed (new) = "+countExecutedNew +
					" ; #executed (updated) = "+countExecutedUpdated +
					" ; #not-run (already executed) = "+countAlreadyExecuted +
					" [elapsed = "+(System.currentTimeMillis()-initTime)+"ms]");
		}
	}

	void executeScript(String dir, String script) throws IOException, SQLException {
		if(script.endsWith(".sql")) {
			executeFileContents(dir, script);
		}
		else if(script.endsWith(".properties")) { // import.properties
			File file = new File(dir, script);
			
			Importer importer = ImporterUtils.getImporter(file);
			importer.setConnection(conn);
			importer.importData();
		}
		// XXX: diff.xml, diff.json
		else {
			String message = "Unknown file type: "+script;
			log.warn(message);
			throw new IllegalArgumentException(message);
		}
	}

	/*
	 * see also: tbrugz.sqldump.sqlrun.StmtProc.execFile()
	 */
	void executeFileContents(String dir, String script) throws IOException, SQLException {
		File file = new File(dir, script);
		Statement st = conn.createStatement();
		Tokenizer scanner = TokenizerStrategy.getDefaultTokenizer(file, charset);
		int stmtCount = 0;
		long updateCount = 0;
		for(String sql: scanner) {
			if(!TokenizerUtil.containsSqlStatmement(sql)) { continue; }
			/*if(replacePropsOnFileContents) {
				//replacing ${...} parameters
				sql = ParametrizedProperties.replaceProps(sql, papp);
			}*/
			try {
				updateCount += st.executeUpdate(sql);
			}
			catch(SQLException e) {
				log.warn("error executing script: "+e+"\nsql = "+sql);
				throw e;
			}
			stmtCount++;
		}
		log.info("script '"+script+"' executed [#statements="+stmtCount+"; #updates="+updateCount+"]");
	}

}
