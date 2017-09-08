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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.log4j.Logger;

/**
 * PageRankDriver class includes main and run methods. It includes 4 jobs run
 * sequentially in order to compute the page rank. First job is used to count
 * the total number of pages in the document and the Second job is used to parse
 * links and create a link graph from the Wikipedia pages, the next job
 * calculates the page ranks iteratively from the existing page rank values, and
 * the final job sanitizes, sorts according to the page rank values and outputs
 * the Page rank values of the top 100 pages in the wiki document and saves the
 * results to the output location in HDFS.
 *
 * @author Sampath Kumar
 * @version 1.0
 * @since 2017-03-18
 */

public class PageRankDriver extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(PageRankDriver.class);

	/*
	 * The main method invokes the PageRankDriver ToolRunner, which creates and
	 * runs a new instance of PageRankDriver job.
	 */

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new PageRankDriver(),
				args));
	}

	@Override
	public int run(String[] args) throws Exception {
		String output = null;
		/*
		 * Run the First MapReduce Job, retrieving the total number of documents
		 * from the wikipedia pages for calculating the initial page rank.
		 */

		boolean getTotalNoOfDocs = getTotalCount(args[0], args[1]);
		Path totalNoOfDocsinput = new Path(args[1] + "/part-r-00000");
		int noOfDocs = getCount(totalNoOfDocsinput); //retrieving the total no of documents
		LOG.info("No of Docs**" + noOfDocs);
		double initialPageRank = (double) 1 / noOfDocs; //calculate initial page rank
		if (!getTotalNoOfDocs)
			return 1;
		/*
		 * Run the Second MapReduce Job, parsing links from the wikipedia pages
		 * and creating a link graph
		 */
		boolean createdLinkGraph = createLinkGraph(args[0], args[2],
				initialPageRank);
		/*
		 * Deleting the intermediate/output directories between executions
		 * of the program
		 */
		if (createdLinkGraph) {
			FileSystem fs = FileSystem.get(getConf());
			if (fs.exists(new Path(args[1]))) {
				fs.delete(new Path(args[1]), true);
			}
		}
		if (!createdLinkGraph)
			return 1;
		/*
		 * Run the third MapReduce Job, calculating new pageranks from existing
		 * values. This job runs in an iterative manner, with each iteration the
		 * pagerank value becomes more and more accurate.
		 */
		for (int i = 0; i < 10; i++) {
			String input;
			if (i == 0) {
				input = args[2];
			} else {
				input = args[2] + i;
			}
			output = args[2] + (i + 1);
			boolean isProcessed = processPageRank(input, output);
			/*
			 * Deleting the intermediate/output directories between executions
			 * of the program
			 */
			if (isProcessed) {
				FileSystem fs = FileSystem.get(getConf());
				if (fs.exists(new Path(input))) {
					fs.delete(new Path(input), true);
				}
			}
			if (!isProcessed)
				return 1;
		}
		/* Run the final MapReduce Job, which cleans and sorts the page rank values */
		boolean isSorted = cleanAndSorting(output, args[3]);
		/*
		 * Deleting the intermediate/output directories between executions of
		 * the program
		 */
		if (isSorted) {
			FileSystem fs = FileSystem.get(getConf());
			if (fs.exists(new Path(output))) {
				fs.delete(new Path(output), true);
			}
		}
		if (!isSorted)
			return 1;
		return 0;
	}

	// To count total number of pages in the document
	private boolean getTotalCount(String input, String output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(getConf(), "totalCount");
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setMapperClass(TotalDocsCountMapper.class);
		job.setReducerClass(TotalDocsCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		return job.waitForCompletion(true);
	}

	// to get the total number of documents from the file
	private static int getCount(Path args) throws IOException {
		Configuration config = new Configuration();
		FileSystem fs = args.getFileSystem(config);
		BufferedReader br = new BufferedReader(new InputStreamReader(
				fs.open(args)));
		String line;
		String count = "";
		line = br.readLine();
		if (line != null && !line.isEmpty()) {
			count = line.split("=")[1];
		}
		return Integer.parseInt(count.trim());
	}

	// Creating Link Graph
	private boolean createLinkGraph(String input, String output,
			double initialPR) throws IOException, ClassNotFoundException,
			InterruptedException {
		Configuration config = new Configuration();
		config.setDouble("PageRank", initialPR);
		Job job = Job.getInstance(config, "createLinkGraph");
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setInputFormatClass(XmlInputFormat.class); // setting the input format of the link graph generator as Xml format.
		job.setMapperClass(LinkGraphParseMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class); // setting the output format of the link graph generator to sequence file format.
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(LinkGraphParseReducer.class);
		return job.waitForCompletion(true);
	}

	// processing page rank values
	private boolean processPageRank(String input, String output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(getConf(), "processPageRank");
		job.setJarByClass(this.getClass());
		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setInputFormatClass(SequenceFileInputFormat.class); // setting the input format of the page rank processing job as Sequence file format.
		job.setOutputFormatClass(SequenceFileOutputFormat.class); // setting the output format of the page rank processing job as Sequence file format.
		job.setMapperClass(ProcessPageRankMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(ProcessPageRankReducer.class);
		return job.waitForCompletion(true);
	}

	// cleaning and sorting the page rank values
	private boolean cleanAndSorting(String input, String output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(getConf(), "cleanAndSorting");
		job.setJarByClass(this.getClass());
		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setMapperClass(RankSortMapper.class);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(SequenceFileInputFormat.class); // setting the input format of the clean and sorting job as Sequence file format.
		job.setOutputFormatClass(TextOutputFormat.class); // setting the output format of the clean and sorting job back to Text format.
		job.setSortComparatorClass(sortComparator.class); // Sort comparator class to sort the page rank results in the descending order.
		job.setReducerClass(RankSortReducer.class);
		return job.waitForCompletion(true);
	}
}

