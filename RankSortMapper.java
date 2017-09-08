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
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import java.io.IOException;

/**
 * RankSortMapper class processes individual lines and strips away unwanted data
 * and writes the page names and their corresponding page rank values.
 * 
 * @author Sampath Kumar
 * @version 1.0
 * @since 2017-03-18
 */

public class RankSortMapper extends Mapper<Text, Text, DoubleWritable, Text> {

	private static final Logger LOG = Logger.getLogger(RankSortMapper.class);

	@Override
	public void map(Text key, Text value, Context context) throws IOException,
			InterruptedException {
		// Get the respective page and split by tabs
		String prAndLinks = value.toString();
		String[] splits = prAndLinks.split("\\t");
		// Get the rank and the current page, stripping the pages at the end
		double rank = Double.parseDouble(splits[0]);
		String page = key.toString();
		// Output by mapping the Page rank as the key to the Mapper for sorting the results.
		context.write(new DoubleWritable(rank), new Text(page));
	}
}
