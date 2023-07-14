package tbrugz.sqldump.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class FileUtils {

	static final Log log = LogFactory.getLog(FileUtils.class);
	
	public static List<String> getFilesRegex(File dir, String fileRegex) {
		if(dir==null) {
			log.warn("dir cannot be null");
			return null;
		}
		List<String> ret = new ArrayList<String>();
		String[] files = dir.list();
		if(files==null) {
			return null;
		}
		for(String file: files) {
			if(file.matches(fileRegex)) {
				ret.add(dir.getAbsolutePath()+File.separator+file);
			}
		}
		return ret;
	}

	// https://docs.oracle.com/javase/7/docs/api/java/nio/file/Files.html#newDirectoryStream(java.nio.file.Path,%20java.lang.String)
	/*public static List<Path> getFilesGlob(File dir, String fileGlob) throws IOException {
		//FileSystem fs = FileSystems.getDefault();
		//fs.getPathMatcher("glob:"+fileGlob);
		List<Path> ret = new ArrayList<>();
		try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir.toPath(), fileGlob)) {
			for (Path entry: stream) {
				//entry.toFile();
				ret.add( entry );
			}
		}
		return ret;
	}*/

	/*
	public static List<String> getFilesGlobAsString(File dir, String fileGlob) throws IOException {
		List<String> ret = new ArrayList<>();
		Path dirPath = dir.toPath();
		//dirPath.
		log.info("dirPath: "+dirPath+" fileGlob: "+fileGlob);
		try (DirectoryStream<Path> stream = Files.newDirectoryStream(dirPath, fileGlob)) {
			for (Path entry: stream) {
				log.info("entry: "+entry);
				ret.add( entry.toString() );
			}
		}
		return ret;
	}
	*/

	public static List<String> getFilesGlobAsString(File dir, String fileGlob) throws IOException {
		Path origPath = dir.toPath();
		//String finalGlob = Paths.get( origPath + File.separator + fileGlob ).toString();
		String finalGlob = origPath + File.separator + fileGlob;
		Finder finder = new Finder(finalGlob);
		
		//log.info("getFilesGlobAsString: dir: "+dir+" ; origPath: "+origPath+" ; fileGlob: "+fileGlob+" finalGlob: "+finalGlob);
		Files.walkFileTree(origPath, finder);
		return finder.getFiles();
	}

	static class Finder extends SimpleFileVisitor<Path> {
	
		static final boolean isWindows = System.getProperty("os.name").toLowerCase().startsWith("win");
		static final String GLOB = "glob:";
		static final char WIN_ASTERISK_SUB = '$';

		private final PathMatcher matcher;
		//private final Path patternParentPath;
		private final Path pathPattern;
		private int pathPatternNameCount;
		final List<String> files = new ArrayList<>();
		
		/*
		static String getBasePath(String pattern) {
			Pattern ptrn = Pattern.compile("[*?\\[\\{]");
			Matcher m = ptrn.matcher(pattern);
			if (m.find()) {
				int position = m.start();
				return pattern.substring(0, position);
			}
			return pattern;
		}
		*/

		static String getPathUntilDoubleAsterisk(String pattern) {
			int idx = pattern.indexOf("**");
			if(idx>0) {
				return pattern.substring(0, idx+2);
			}
			return pattern;
		}
		
		Finder(String pattern) {
			matcher = FileSystems.getDefault().getPathMatcher(GLOB + normalizeComparatorPath( pattern) ); // **/
			//patternParentPath = Paths.get( getBasePath(pattern) );
			// using getPathUntilDoubleAsterisk() because "**" crosses directory boundaries
			// see: https://docs.oracle.com/javase/7/docs/api/java/nio/file/FileSystem.html#getPathMatcher(java.lang.String)
			String path = normalizePath( getPathUntilDoubleAsterisk(pattern) );
			//log.info("path: "+path+" ; pattern: "+pattern);
			pathPattern = Paths.get( path );
			pathPatternNameCount = pathPattern.getNameCount();
			//log.info("pathPattern: "+pathPattern+" ; pathPatternNameCount: "+pathPatternNameCount);
		}
		
		void find(Path file) {
			//log.debug("find: f: "+file);
			if (matcher.matches(file)) {
				//log.debug("find: match: "+file);
				files.add(file.toString());
			}
			/*
			Path name = file.getFileName();
			if (name != null && matcher.matches(name)) {
				files.add(name.toString());
			}
			*/
		}
		
		static String normalizePath(String path) {
			if(isWindows) {
				if(path.indexOf(WIN_ASTERISK_SUB)>=0) {
					throw new IllegalArgumentException("Illegal char <"+WIN_ASTERISK_SUB+">");
				}
				path = path.replace('*', WIN_ASTERISK_SUB);
				path = path.replace('/', '\\');
				return path;
			}
			return path;
		}

		static String normalizeComparatorPath(String path) {
			if(isWindows) {
				path = path.replace('/', '\\');
				path = path.replace(WIN_ASTERISK_SUB, '*');
				path = path.replaceAll(Pattern.quote("\\"), Matcher.quoteReplacement("\\\\"));
				return path;
			}
			return path;
		}

		List<String> getFiles() {
			return files;
		}
		
		@Override
		public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
			find(file);
			return FileVisitResult.CONTINUE;
		}
		
		@Override
		public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
			//log.info("preVisitDirectory: "+dir);
			String comparatorPath = pathPattern.toString();
			int dirNameCount = dir.getNameCount();
			if(dirNameCount < pathPatternNameCount) {
				comparatorPath = getSubpathWithRoot(pathPattern, dirNameCount).toString();
				/*comparatorPath = pathPattern.subpath(0, dirNameCount);
				if(pathPattern.isAbsolute()) {
					comparatorPath = Paths.get(pathPattern.getRoot().toString()+comparatorPath);
				}*/
			}
			comparatorPath = normalizeComparatorPath( comparatorPath );
			PathMatcher dirMatcher = FileSystems.getDefault().getPathMatcher(GLOB + comparatorPath);
			if(!dirMatcher.matches(dir)) {
				//log.info("SKIP: dir: "+dir+" ;; comparatorPath: "+comparatorPath);
				return FileVisitResult.SKIP_SUBTREE;
			}
			else {
				//log.info("NO-SKIP: dir: "+dir+" ;; comparatorPath: "+comparatorPath);
			}
			
			/*
			if(!patternParentPath.startsWith(dir) && !dir.startsWith(patternParentPath)) {
				//System.out.println("SKIP: dir: "+dir+" ;; patternParentPath: "+patternParentPath);
				return FileVisitResult.SKIP_SUBTREE;
			}
			*/
			return FileVisitResult.CONTINUE;
		}
	
		@Override
		public FileVisitResult visitFileFailed(Path file, IOException exc) {
			log.warn(exc);
			return FileVisitResult.CONTINUE;
		}
	}

	public static File getInitDirForPath(String path) {
		Path p = Paths.get(path);
		if(p.isAbsolute()) {
			return p.getRoot().toFile();
		}
		return new File(System.getProperty("user.dir"));
	}
	
	public static Path getSubpathWithRoot(Path path, int endIndex) {
		Path subpath = path.subpath(0, endIndex);
		if(path.isAbsolute()) {
			subpath = Paths.get(path.getRoot().toString()+subpath);
		}
		return subpath;
	}

}
