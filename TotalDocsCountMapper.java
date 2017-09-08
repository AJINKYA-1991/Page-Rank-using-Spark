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

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * TotalDocsCountMapper class processes individual lines on the input corpus,
 * and emits the constant key and value 1. 
 * 
 * @author Sampath Kumar
 * @version 1.0
 * @since 2017-03-18
 */
public class TotalDocsCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);

	public void map(LongWritable offset, Text lineText, Context context)
			throws IOException, InterruptedException {
		String line = lineText.toString();
		if (!(line.isEmpty()) && line != null) {
			context.write(new Text("Count= "), one);
		}
	}
}

