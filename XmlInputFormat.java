/*=============================================================================
|   Assignment:  Individual assignment: Programming - 3
|       Author:  AJINKYA MOHAPATRA
|       Grader:  Walid Shalaby
|
|       Course:  ITCS 6190
|   Instructor:  Srinivas Akella
|     Due Date:  March 23 at 11:59PM
|
|     Language:  Java 
|     Version :  1.8.0_101
|                
| Deficiencies:  No logical errors.
 *===========================================================================*/

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import java.io.IOException;

/**
 * XmlInputFormat class to read through the given XML document to output XML
 * blocks specified by the start and the end tags.
 * 
 * @author Sampath Kumar
 * @version 1.0
 * @since 2017-03-18
 */

public class XmlInputFormat extends TextInputFormat {

	public static final String START_TAG_KEY = "<title>";
	public static final String END_TAG_KEY = "</text>";

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) {
		try {
			return new XmlRecordReader((FileSplit) split, context);
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage(), e.getCause());
		}
	}

	/**
	 * XMLRecordReader class to read through a given xml document to output xml
	 * blocks as records as specified by the start tag and end tag
	 */
	public static class XmlRecordReader extends
			RecordReader<LongWritable, Text> {
		private final byte[] startTag;
		private final byte[] endTag;
		private final long start;
		private final long end;
		private final FSDataInputStream fsin;
		private final DataOutputBuffer buffer = new DataOutputBuffer();

		private LongWritable key = new LongWritable();
		private Text value = new Text();

		public XmlRecordReader(FileSplit split, TaskAttemptContext context)
				throws IOException {
			startTag = START_TAG_KEY.getBytes("utf-8");
			endTag = END_TAG_KEY.getBytes("utf-8");
			// open the file and seek to the start of the split
			start = split.getStart();
			end = start + split.getLength();
			Path file = split.getPath();
			FileSystem fs = file.getFileSystem(context.getConfiguration());
			fsin = fs.open(split.getPath());
			fsin.seek(start);
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (fsin.getPos() < end) {
				if (readUntilMatch(startTag, false)) {
					try {
						buffer.write(startTag);
						if (readUntilMatch(endTag, true)) {
							key.set(fsin.getPos());
							value.set(buffer.getData(), 0, buffer.getLength());
							return true;
						}
					} finally {
						buffer.reset();
					}
				}
			}
			return false;
		}

		@Override
		public LongWritable getCurrentKey() throws IOException,
				InterruptedException {
			return key;
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		@Override
		public void close() throws IOException {
			fsin.close();
		}

		@Override
		public float getProgress() throws IOException {
			return (fsin.getPos() - start) / (float) (end - start);
		}

		private boolean readUntilMatch(byte[] match, boolean withinBlock)
				throws IOException {
			int i = 0;
			while (true) {
				int b = fsin.read();
				// end of file:
				if (b == -1)
					return false;
				// save to buffer:
				if (withinBlock)
					buffer.write(b);

				// check if we're matching:
				if (b == match[i]) {
					i++;
					if (i >= match.length)
						return true;
				} else
					i = 0;
				// see if we've passed the stop point:
				if (!withinBlock && i == 0 && fsin.getPos() >= end)
					return false;
			}
		}
	}
}
