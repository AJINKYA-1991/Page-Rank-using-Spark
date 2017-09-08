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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * The reducer processes each pair, and sums the values which gives the total number of pages in the document.
 */

public class TotalDocsCountReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	public void reduce(Text key, Iterable<IntWritable> counts, Context context)
			throws IOException, InterruptedException {
		int sum = 0;

		for (IntWritable count : counts) {
			sum += count.get();
		}
		context.write(key, new IntWritable(sum));
	}
}
