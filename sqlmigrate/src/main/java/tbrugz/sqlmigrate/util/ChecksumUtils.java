package tbrugz.sqlmigrate.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

public class ChecksumUtils {

	public static long getChecksumCRC32(InputStream stream) throws IOException {
		return getChecksumCRC32(stream, 8192);
	}
	
	/*
	 * see: https://www.baeldung.com/java-checksums#checksum-from-input-stream
	 */
	public static long getChecksumCRC32(InputStream stream, int bufferSize) throws IOException {
		CheckedInputStream checkedInputStream = new CheckedInputStream(stream, new CRC32());
		byte[] buffer = new byte[bufferSize];
		while (checkedInputStream.read(buffer, 0, buffer.length) >= 0) {}
		return checkedInputStream.getChecksum().getValue();
	}

}
