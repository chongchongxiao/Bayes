package format;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

/**
 * Treats keys as offset in file and value as line.
 */
public class PriorProRecordReader extends RecordReader<LongWritable, Text> {

	private Text value = null;
	private boolean isReaded = false;//文件是否已读取
	private String className = null;

	public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
		value = new Text();
		className = getClassName(genericSplit.toString());// huoqu lei ming
	}

	//这里设置每个文件仅读取一次
	public boolean nextKeyValue() throws IOException {
		if(!isReaded) {
			isReaded=true;
			return true;
		}else {
			return false;
		}
	}

	@Override
	public LongWritable getCurrentKey() {
		return null;
	}

	//返回类名
	@Override
	public Text getCurrentValue() {
		value.set(className);
		return value;
	}

	// 从文件路径中获取类别名称
	private String getClassName(String str) {
		String[] subStr = str.split("/");
		return subStr[6];
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

}