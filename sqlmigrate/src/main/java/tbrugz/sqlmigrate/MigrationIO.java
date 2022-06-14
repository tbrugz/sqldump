package tbrugz.sqlmigrate;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqlmigrate.util.ChecksumUtils;

public class MigrationIO {

	static final Log log = LogFactory.getLog(MigrationIO.class);

	//XXX: sorts alphanumerically, not by dotted version
	/*public static String[] listFilesSorted(File dir) {
		String[] files = dir.list();
		Arrays.sort(files);
		return files;
	}*/
	
	//XXX: repeatable migrations? different method, since they may be on different dir
	public static List<Migration> listMigrations(File dir, boolean getChecksum) throws FileNotFoundException, IOException {
		List<Migration> migs = new ArrayList<>();
		//String[] files = listFilesSorted(dir);
		String[] files = dir.list();
		for(String f: files) {
			if(f.endsWith(".sql")) {
				//log.info("file: "+f);
				int idx = f.indexOf('_');
				if(idx > 0) {
					String strVersion = f.substring(0, idx);
					//DotVersion dv = DotVersion.getDotVersion(strVersion);
					//String rest = f.substring(idx+1);
					Long checksum = null;
					if(getChecksum) {
						checksum = ChecksumUtils.getChecksumCRC32(new FileInputStream(new File(dir, f)));
					}
					if(!DotVersion.isValidVersion(strVersion)) {
						//log.warn("not a valid version: "+strVersion+" [file: "+f+"]");
						log.info("ignored file (invalid version: ["+strVersion+"]): "+f);
						continue;
					}
					Migration m = new Migration(strVersion, f, checksum);
					migs.add(m);
					//log.info("migration file: "+f+" ; mig: "+m);
				}
				else {
					// repeatable?
					//log.info("repeatable migration file? file: "+f);
					log.info("ignored file (no version): "+f);
				}
			}
			//XXX: .properties file
			else {
				log.info("ignored file (unknown extension): "+f);
			}
		}
		return migs;
	}

	/*
	public static boolean hasDuplicatedVersion0(List<Migration> migs) {
		Migration lastMig = null;
		for(Migration m: migs) {
			if(lastMig!=null) {
				//System.out.println("lm="+lastMig.getVersion()+" ; m="+m.getVersion());
				if(lastMig.getVersion().equals(m.getVersion())) {
					log.info("hasDuplicates: last="+lastMig+" ; this="+m);
					return true;
				}
			}
			lastMig = m;
		}
		return false;
	}
	*/
	
	public static void sortMigrations(List<Migration> migrations) {
		migrations.sort(new Migration.MigrationComparator());
	}

	public static boolean hasDuplicatedVersions(List<Migration> migs) {
		int originalSize = migs.size();
		Set<DotVersion> versions = new HashSet<>();
		for(Migration m: migs) {
			versions.add(m.getVersion());
		}
		log.debug("hasDuplicates: originalSize="+originalSize+" ; versionsSize="+versions.size());
		return originalSize != versions.size();
	}

	public static Set<DotVersion> getDuplicatedVersions(List<Migration> migs) {
		Set<DotVersion> versions = new HashSet<>();
		Set<DotVersion> duplicates = new TreeSet<>();
		for(Migration m: migs) {
			if(versions.contains(m.getVersion())) {
				duplicates.add(m.getVersion());
			}
			versions.add(m.getVersion());
		}
		return duplicates;
	}
	
	public static Set<DotVersion> getVersionSet(List<Migration> migs) {
		Set<DotVersion> versions = new HashSet<>();
		for(Migration m: migs) {
			versions.add(m.getVersion());
		}
		return versions;
	}
	
	public static Set<DotVersion> diffVersions(Set<DotVersion> vs1, Set<DotVersion> vs2) {
		Set<DotVersion> ret = new TreeSet<>(vs1);
		ret.removeAll(vs2);
		return ret;
	}

	public static List<Migration> getMigrationsFromVersion(List<Migration> migs, DotVersion version) {
		List<Migration> ret = new ArrayList<>();
		for(Migration m: migs) {
			if(version.equals(m.getVersion())) {
				ret.add(m);
			}
		}
		return ret;
	}
	
}
