package tbrugz.sqlmigrate;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldiff.model.SchemaDiff;
import tbrugz.sqldump.util.Utils;
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
	static final String PROP_BASELINE = PRODUCT_NAME + "." + "baseline";
	static final String PROP_BASELINE_VERSION = PRODUCT_NAME + "." + "baseline-version";

	static final String DEFAULT_MIGRATION_TABLE = "SQLMIGRATE_HISTORY"; //"sqlmigrate_history"; //"sqlm_changelog";
	static final String DEFAULT_MIGRATIONS_DIR = ".";
	
	public enum MigrateAction {
		SETUP,
		STATUS,
		MIGRATE,
	}

	// common properties
	MigrateAction action = null;
	String migrationsSchemaName = null;
	String migrationsTable = DEFAULT_MIGRATION_TABLE;
	String versionedMigrationsDir = null;
	//String repeatableMigrationsDir = null;
	
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
	
	@Override
	public void procProterties() {
		String actionStr = papp.getProperty(PROP_ACTION);
		if(actionStr==null) {
			throw new IllegalArgumentException("null migration action");
		}
		try {
			action = MigrateAction.valueOf(actionStr.toUpperCase());
		}
		catch(IllegalArgumentException e) {
			throw new IllegalArgumentException("unknown action '"+actionStr+"'", e);
		}
		migrationsTable = papp.getProperty(PROP_MIGRATION_TABLE, DEFAULT_MIGRATION_TABLE);
		migrationsSchemaName = papp.getProperty(PROP_MIGRATION_TABLE_SCHEMA);
		versionedMigrationsDir = papp.getProperty(PROP_MIGRATIONS_DIR, DEFAULT_MIGRATIONS_DIR);
		baseline = Utils.getPropBool(papp, PROP_BASELINE, baseline);
		baselineVersion = papp.getProperty(PROP_BASELINE_VERSION, baselineVersion);
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
		} catch (IOException | SQLException e) {
			log.warn("doIt: Error: "+e, e);
			throw new RuntimeException(e);
		}
	}
	
	/*
	 * setup: create (if needed) the migration table
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
			doSetupBaseline();
		}
	}
	
	void doSetupBaseline() throws SQLException, FileNotFoundException, IOException {
		log.info(">> setup::baseline [dry-run="+isDryRun()+"]");
		long initTime = System.currentTimeMillis();
		boolean getChecksum = false;
		DotVersion upperVersion = null; 
		if(baselineVersion!=null) {
			upperVersion = DotVersion.getDotVersion(baselineVersion);
			log.info("baseline-version = "+upperVersion);
		}
		
		MigrationDao md = new MigrationDao(migrationsSchemaName, migrationsTable);
		List<Migration> mds = md.listMigrations(conn);
		MigrationIO.sortMigrations(mds);

		log.info("grabbing fs migrations from: "+versionedMigrationsDir);
		List<Migration> mios = MigrationIO.listMigrations(new File(versionedMigrationsDir), getChecksum);
		MigrationIO.sortMigrations(mios);

		boolean hasDbDuplicates = MigrationIO.hasDuplicatedVersions(mds);
		boolean hasFsDuplicates = MigrationIO.hasDuplicatedVersions(mios);
		if(hasDbDuplicates || hasFsDuplicates) {
			log.warn("duplicated migration versions found "+
					"[hasDbDuplicates="+hasDbDuplicates+";hasFsDuplicates="+hasFsDuplicates+"]. can't proceed");
			return;
		}
		
		Set<DotVersion> vdb = MigrationIO.getVersionSet(mds);
		long countSaves = 0, countRemoves = 0, countUnchanged = 0;
		for(Migration m: mios) {
			DotVersion fsmv = m.getVersion();
			if(upperVersion!=null && upperVersion.compareTo(fsmv) < 0) {
				if(vdb.contains(fsmv)) {
					if(!isDryRun()) {
						log.info("version '"+fsmv+"' greater than baseline-version '"+upperVersion+"' and in database. removing");
						md.removeByVersion(m, conn);
					}
					else {
						log.info("version '"+fsmv+"' greater than baseline-version '"+upperVersion+"' and in database. would be removed");
					}
					countRemoves += 1;
				}
				else {
					log.info("version '"+fsmv+"' greater than baseline-version '"+upperVersion+"'");
				}
			}
			else if(vdb.contains(fsmv)) {
				// db already has this migration, do nothing
				log.info("migration "+m+" already in database. doing nothing");
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
		log.info("baseline: # migrations saved = "+countSaves +
				" ; # migrations removed = "+countRemoves +
				" ; # migrations unchanged = "+countUnchanged +
				" [elapsed = "+(System.currentTimeMillis()-initTime)+"ms]");
	}
	
	/*
	 * show if migration table is present
	 * show executed & pending migrations
	 */
	public void doStatus() throws SQLException, FileNotFoundException, IOException {
		log.info(">> status action");
		boolean getChecksum = false;
		
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
		List<Migration> mds = md.listMigrations(conn);
		MigrationIO.sortMigrations(mds);
		if(log.isDebugEnabled()) {
			log.debug(">> db migrations:");
			for(Migration m: mds) {
				log.debug(m);
			}
		}
		
		log.info("grabbing fs migrations from: "+versionedMigrationsDir);
		List<Migration> mios = MigrationIO.listMigrations(new File(versionedMigrationsDir), getChecksum);
		MigrationIO.sortMigrations(mios);
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
		Set<DotVersion> vdb0 = MigrationIO.diffVersions(vdb, vio);
		if(vdb0.size()>0) {
			log.warn(vdb0.size()+" executed migrations with no filesystem correspondence ["+vdb0+"]");
		}
		
		// show pending migrations
		Set<DotVersion> vio0 = MigrationIO.diffVersions(vio, vdb);
		if(vio0.size()>0) {
			log.info(vio0.size()+" pending migrations: "+vio0);
		}
	}
	
	/*
	 * migrate: execute pending migrations + update migration table
	 * dry-run: create sqlrun properties file without running
	 * options: run-versioned-migrations ; run-repeatable-migrations
	 */
	//TODO: migrate
	public void doMigrate() {
		
	}

}
