package tbrugz.sqldiff;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Reader;

import tbrugz.sqldiff.model.Diff;

public interface DiffGrabber {
	String type();
	Diff grabDiff(Reader reader);
	Diff grabDiff(File file) throws FileNotFoundException;
}
