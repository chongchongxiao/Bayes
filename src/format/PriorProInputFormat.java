package format;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class PriorProInputFormat extends FileInputFormat<LongWritable, Text> {

	  @Override
	  public RecordReader<LongWritable, Text> 
	    createRecordReader(InputSplit split,
	                       TaskAttemptContext context) {
		  return new PriorProRecordReader();
	  }

	  //读取文件数量，避免文件切割
	  @Override
	  protected boolean isSplitable(JobContext context, Path file) {
	    return false;
	  }

	}