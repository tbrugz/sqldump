package tbrugz.util;

public class LongFactory extends GenericFactory<Long> {
	long initialValue = 0L;
	
	@Override
	public Long getInstance() {
		return initialValue;
	}
}
