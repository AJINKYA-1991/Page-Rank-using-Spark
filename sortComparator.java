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
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * sortComparator class to sort the results in the descending order.
 * 
 * @author Sampath Kumar
 * @version 1.0
 * @since 2017-03-18
 */

public class sortComparator extends WritableComparator {

	protected sortComparator() {
		super(DoubleWritable.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable o1, WritableComparable o2) {
		DoubleWritable k1 = (DoubleWritable) o1;
		DoubleWritable k2 = (DoubleWritable) o2;
		int cmp = k1.compareTo(k2);
		return -1 * cmp;
	}
}
