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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * The reducer processes each pair, stores pages, initial page rank and outgoing
 * links from each map call.
 * 
 * @author Sampath Kumar
 * @version 1.0
 * @since 2017-03-18
 */

public class LinkGraphParseReducer extends Reducer<Text, Text, Text, Text> {
	private static final Logger LOG = Logger
			.getLogger(LinkGraphParseReducer.class);

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Configuration con = context.getConfiguration();
		double initialpageRk = con.getDouble("PageRank", 0);
		LOG.info("Initial PR value**********" + initialpageRk);
		String pageRK = Double.toString(initialpageRk);
		String finalValue = pageRK + "\t";
		boolean first = true;
		for (Text value : values) {
			if (!first) {
				finalValue += "#####";
			}
			finalValue += value.toString();
			first = false;
		}
		context.write(key, new Text(finalValue));
	}
}
