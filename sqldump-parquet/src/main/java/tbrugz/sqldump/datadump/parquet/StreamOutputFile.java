package tbrugz.sqldump.datadump.parquet;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

public class StreamOutputFile implements OutputFile {

	private final OutputStream outputStream;

	public StreamOutputFile(OutputStream outputStream) {
		this.outputStream = outputStream;
	}

	@Override
	public PositionOutputStream create(long blockSize) {
		return new PositionOutputStream() {
			private long pos = 0;

			@Override
			public long getPos() {
				return pos;
			}

			@Override
			public void write(int b) throws IOException {
				outputStream.write(b);
				pos++;
			}

			@Override
			public void write(byte[] b, int off, int len) throws IOException {
				outputStream.write(b, off, len);
				pos += len;
			}

			@Override
			public void flush() throws IOException {
				outputStream.flush();
			}

			@Override
			public void close() throws IOException {
				outputStream.close();
			}
		};
	}

	@Override
	public PositionOutputStream createOrOverwrite(long blockSize) {
		return create(blockSize);
	}

	@Override
	public boolean supportsBlockSize() {
		return false;
	}

	@Override
	public long defaultBlockSize() {
		return 128 * 1024 * 1024;
	} // 128MB
}
