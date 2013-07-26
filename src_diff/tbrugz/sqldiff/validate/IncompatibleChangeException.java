package tbrugz.sqldiff.validate;

public class IncompatibleChangeException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public IncompatibleChangeException(String message) {
		super(message);
	}
}
