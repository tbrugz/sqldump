/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tbrugz.util.logback;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.pattern.CompositeConverter;

/**
 * Heavily based on:
 * https://github.com/spring-projects/spring-boot/blob/main/spring-boot-project/spring-boot/src/main/java/org/springframework/boot/logging/logback/ColorConverter.java
 */
public class LevelColorConverter extends CompositeConverter<ILoggingEvent> {

	public enum AnsiColor {
		// styles
		NORMAL("0"),
		BOLD("1"),
		FAINT("2"),
		ITALIC("3"),
		UNDERLINE("4"),

		// colors
		DEFAULT("39"),
		GREEN("32"),
		RED("31"),
		YELLOW("33"),
		BRIGHT_WHITE("97")
		;

		private final String code;

		AnsiColor(String code) {
			this.code = code;
		}

		@Override
		public String toString() {
			return this.code;
		}
	}

	private static final String ENCODE_START = "\033[";
	private static final String ENCODE_END = "m";
	private static final String RESET = "0;" + AnsiColor.DEFAULT;

	private static final Map<Integer, AnsiColor> LEVELS;

	static {
		Map<Integer, AnsiColor> ansiLevels = new HashMap<>();
		ansiLevels.put(Level.ERROR_INTEGER, AnsiColor.RED);
		ansiLevels.put(Level.WARN_INTEGER, AnsiColor.YELLOW);
		LEVELS = Collections.unmodifiableMap(ansiLevels);
	}

	@Override
	protected String transform(ILoggingEvent event, String in) {
		//AnsiColor style = AnsiColor.NORMAL;
		AnsiColor style = AnsiColor.BOLD;
		AnsiColor color = LEVELS.get(event.getLevel().toInteger());
		if(color==null) {
			color = AnsiColor.BRIGHT_WHITE;
			style = AnsiColor.FAINT;
			//style = AnsiColor.BOLD;
		}
		return toAnsiString(in, new AnsiColor[]{style, color} );
	}

	protected String toAnsiString(String in, AnsiColor[] element) {
		return ENCODE_START + element[0] + ";" + element[1] + ENCODE_END +
				in +
				ENCODE_START + RESET + ENCODE_END;
	}

}
