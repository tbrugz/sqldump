package tbrugz.sqlmigrate;

import java.util.Comparator;

public class Migration {

	DotVersion version;
	String script;
	Long crc32;
	//executed/run status (executed/not executed);
	//Boolean hasRun; //needed? if migration is present, it has run...
	//(script) sync status (avaiable/not avaiable/checksum mismatch) [NOT NEEDED IN DATABASE] ;	
	//Boolean scriptAvaiable;
	//Boolean checksumMatched;
	
	public Migration(String version, String script, Long crc32) {
		this.version = DotVersion.getDotVersion(version);
		this.script = script;
		this.crc32 = crc32;
	}
	
	public DotVersion getVersion() {
		return version;
	}
	
	public String getScript() {
		return script;
	}
	
	public String getChecksumString() {
		if(crc32==null) { return "#null#"; }
		return String.valueOf(crc32);
	}
	
	@Override
	public String toString() {
		//return "Migration[v="+version+";s="+script+"]";
		return "Migration[v="+version+";s="+script+";cs="+crc32+"]";
	}
	
	public boolean matchesChecksum(Migration other) {
		if(crc32==null || other.crc32==null) { return false; }
		return crc32.equals(other.crc32);
	}
	
	public static class MigrationComparator implements Comparator<Migration> {
		@Override
		public int compare(Migration o1, Migration o2) {
			return o1.getVersion().compareTo(o2.getVersion());
		}
	}

	public static class MigrationScriptComparator implements Comparator<Migration> {
		@Override
		public int compare(Migration o1, Migration o2) {
			return o1.script.compareTo(o2.script);
		}
	}
	
}
