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

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import java.io.IOException;

/**
 * RankSortReducer class processes individual pairs, runs a counter till 100 to
 * write the top 100 results to the output location in the HDFS.
 * 
 * @author Sampath Kumar
 * @version 1.0
 * @since 2017-03-18
 */

public class RankSortReducer extends
		Reducer<DoubleWritable, Text, Text, DoubleWritable> {

	private static final Logger LOG = Logger
			.getLogger(ProcessPageRankMapper.class);
	
	private static final long topResults = 100; // To retrieve the top 100 results

	@Override
	public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		long count = 1;
		// Counter to keep track of the top results
		if (context.getCounter("topPageRankResults", "topPageRankResults") != null) {
			count = context.getCounter("topPageRankResults",
					"topPageRankResults").getValue();
		}
		for (Text val : values) {
			if (count < topResults) {
				count = count + 1;
				context.getCounter("topPageRankResults", "topPageRankResults")
						.setValue(count);
				LOG.info("sort reducer key*****" + val.toString() + "value"
						+ key.toString());
				context.write(val, key);
			}
		}
	}
}
