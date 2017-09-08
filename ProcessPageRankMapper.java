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
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import java.io.IOException;

/**
 * ProcessPageRankMapper class processes individual lines on the link graph, and
 * stores the linked pages, page rank and the total number of outlinks. In
 * addition to this, another emit is made to preserve all the outgoing links to
 * run the process iteratively.
 * 
 * @author Sampath Kumar
 * @version 1.0
 * @since 2017-03-18
 */

public class ProcessPageRankMapper extends Mapper<Text, Text, Text, Text> {

	private static final Logger LOG = Logger
			.getLogger(ProcessPageRankMapper.class);

	@Override
	public void map(Text key, Text value, Context context) throws IOException,
			InterruptedException {
		String pageRankAndOutlinks = value.toString();
		String outLinks;
		String currPg;
		String[] pages;
		int outLinksCount = 0;
		String[] splits = pageRankAndOutlinks.split("\\t");
		String currPage = key.toString();
		String currPageAndPageRank = currPage + "\t" + splits[0] + "\t";
		/*
		 * Ignoring if page contains no out links, which is used to compute page
		 * ranks only for the documents in the corpus
		 */
		if (splits.length < 2 || splits[1] == "") {
			context.write(new Text(currPage), new Text("*****"));
			return;
		}
		outLinks = splits[1];
		pages = outLinks.split("#####");
		outLinksCount = pages.length;
		currPg = currPageAndPageRank + outLinksCount;
		// For each out links, store page, pageRank and totalNumberOfOutLinks
		for (String page : pages) {
			context.write(new Text(page), new Text(currPg));
		}
		// Preserving the original links
		context.write(new Text(currPage), new Text("*****" + outLinks));
	}
}
