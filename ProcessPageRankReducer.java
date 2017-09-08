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
import java.io.IOException;
import org.apache.log4j.Logger;

/**
 * ProcessPageRankReducer class processes each pair, computes the page rank,
 * adds the new page rank and emits again in the same format received by the
 * mapper for processing the next iteration of PageRank.
 * 
 * @author Sampath Kumar
 * @version 1.0
 * @since 2017-03-18
 */

public class ProcessPageRankReducer extends Reducer<Text, Text, Text, Text> {

	private static final Logger LOG = Logger
			.getLogger(ProcessPageRankReducer.class);

	@Override
	public void reduce(Text page, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		double dampingFactor = 0.85; // Damping Factor
		double pageRk = 0;
		String preserveLinks = "";
		for (Text curr : values) {
			String value = curr.toString();
			// If it is a links ***** record, preserve the links
			if (value.startsWith("*")) {
				preserveLinks = value.substring(value.lastIndexOf("*") + 1);
				LOG.info("Preserved links only" + preserveLinks);
			}
			// If it is a normal record, compute the page rank
			else {
				String[] splits = value.split("\\t"); // Find the page rank and number of links for the given page
				double currentPageRank = Double.valueOf(splits[1]);
				int linkCount = Integer.valueOf(splits[2]);
				pageRk += currentPageRank / linkCount; // Sum all the in links and divide by the out degree
			}
		}
		/*
		 * condition to avoid writing the page rank values for the documents not
		 * present in the corpus
		 */
		if (preserveLinks != "") {
			// Calculate page rank by applying the damping factor to the sum
			double pageRank = (1 - dampingFactor) + dampingFactor * pageRk;
			// Add new pagerank to total
			context.write(page, new Text(pageRank + "\t" + preserveLinks));
		}
	}
}
