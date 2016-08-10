package de.bst.five_cities.utils;

import static org.apache.commons.lang3.StringUtils.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.Stream.Builder;

public class CreateDoc {

	public static void main(String[] args) throws Exception {
		generateDoc("src/test/java/de/bst/five_cities/FiveCitiesSparkTutorialTest.java", "README.md");
	}

	public static void generateDoc(String source, String target) throws IOException {
		Stream<String> documentLines = Files.lines(Paths.get(source)) //
				.map(line -> new BlockOfLines(line)) //
				.collect(//
						() -> new LinkedList<>(), //
						CreateDoc::mergeLinesOfSameTypeToBlocks, //
						dontCombine())
				.stream() //
				.flatMap(BlockOfLines::format);

		Files.write(Paths.get(target), (Iterable<String>) documentLines::iterator);
	}

	private static void mergeLinesOfSameTypeToBlocks(LinkedList<BlockOfLines> blocks, BlockOfLines block) {
		if (!blocks.isEmpty() && (block.type == blocks.getLast().type))
			blocks.getLast().merge(block);
		else
			blocks.add(block);
	}

	private static class BlockOfLines {

		static enum LineType {
			CODE, TEXT;

			static LineType of(String line) {
				if (HIDE_PATTERN.matcher(line).matches())
					return CODE;
				else if (COMMENT_PATTERN.matcher(line).matches())
					return TEXT;
				else
					return CODE;
			}

		}

		private static final Pattern COMMENT_PATTERN = Pattern.compile("^\\s*// ?(.*)$");
		private static final Pattern HIDE_PATTERN = Pattern.compile("^\\s*//\\s*!HIDE\\s*$");

		LinkedList<String> lines = new LinkedList<>();
		LineType type;
		boolean hide = false;

		public BlockOfLines(String line) {
			this.lines.add(line);
			this.type = LineType.of(line);
			this.hide = HIDE_PATTERN.matcher(line).matches();
		}

		public void merge(BlockOfLines other) {
			lines.addAll(other.lines);
		}

		@Override
		public String toString() {
			return type.toString() + "\n\t" + join(lines, "\n\t");
		}

		private Stream<String> format() {
			switch (type) {
			case CODE:
				Builder<String> builder = Stream.<String> builder();
				if (!hide) {
					builder.add("```java");
					lines.forEach(l -> builder.add(l));
					builder.add("```");
				}
				return builder.build();
			case TEXT:
				return lines //
						.stream() //
						.map(line -> {
							Matcher matcher = COMMENT_PATTERN.matcher(line);
							return matcher.matches() ? matcher.group(1) : line;
						});
			default:
				throw new IllegalArgumentException();
			}
		}

	}

	private static <A> BiConsumer<A, A> dontCombine() {
		return (a, b) -> {
			throw new UnsupportedOperationException();
		};
	}

}
