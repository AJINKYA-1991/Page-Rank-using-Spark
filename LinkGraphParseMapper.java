/*=============================================================================
|   Assignment:  Individual assignment: Programming - 3
|       Author: AJINKYA MOHAPATRA
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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.log4j.Logger;

/**
 * LinkGraphParseMapper class processes individual lines on the input corpus,
 * parses it and creates a link graph with the initial page rank values.
 * 
 * @author Sampath Kumar
 * @version 1.0
 * @since 2017-03-18
 */

public class LinkGraphParseMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	private static final Logger LOG = Logger
			.getLogger(LinkGraphParseMapper.class);

	// Regex pattern used to identify the links inside the text tag
	private static final Pattern linksPattern = Pattern
			.compile("(?<=[\\[]{2}).+?(?=[\\]])");

	@Override
	public void map(LongWritable key, Text page, Context context)
			throws IOException, InterruptedException {

		String titleString;
		String textString;
		int startTitle = page.find("<title>");
		int endTitle = page.find("</title>", startTitle);
		int startText = page.find("<text");
		startText = page.find(">", startText);
		int endText = page.find("</text>", startText);
		if (startTitle == -1 || endTitle == -1 || startText == -1
				|| endText == -1) {
			titleString = "";
			textString = "";
		}
		// Excluding the title and text tags by adding their positions to the indices
		startTitle += 7;
		startText += 1;

		// Parse the Text to Strings
		titleString = Text.decode(page.getBytes(), startTitle, endTitle
				- startTitle);
		textString = Text.decode(page.getBytes(), startText, endText
				- startText);

		// Parse the text tag (body of the page) for links
		Matcher links = linksPattern.matcher(textString);

		// Loop through the identified links
		while (links.find()) {
			String otherPageLink = links.group();
			// If the link is not a valid link, avoid it
			if (otherPageLink == null || otherPageLink.isEmpty())
				continue;
			// Add valid pages
			context.write(new Text(titleString), new Text(otherPageLink));
		}
	}
}

