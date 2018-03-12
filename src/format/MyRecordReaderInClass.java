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
public class MyRecordReaderInClass extends RecordReader<LongWritable, Text> {  
  private static final Log LOG = LogFactory.getLog(MyRecordReaderInClass.class);  
  
  private CompressionCodecFactory compressionCodecs = null;  
  private long start;  
  private long pos;  
  private long end;  
  private LineReader in;  
  private int maxLineLength;  
  private LongWritable key = null;  
  private Text value = null;  
  private Seekable filePosition;  
  private CompressionCodec codec; //  
  private Decompressor decompressor;  
  
  
  private String className;//ji lu  lei  ming
  private boolean isFilter=false; //shifou   guolv  shuzi
  
  
  public void initialize(InputSplit genericSplit,  
                         TaskAttemptContext context) throws IOException {  
    FileSplit split = (FileSplit) genericSplit;  
    
    className = getClassName(genericSplit.toString());//huoqu  lei ming
    
    Configuration job = context.getConfiguration();  
    this.maxLineLength = job.getInt("mapred.MyRecordReader.maxlength",  
                                    Integer.MAX_VALUE);  
    start = split.getStart();  
    end = start + split.getLength();  
    final Path file = split.getPath();  
    compressionCodecs = new CompressionCodecFactory(job);  
    //根据job的配置信息，和split的信息，获取到读取实体文件的信息，这里包括文件的压缩信息。  
    //这里压缩的code有：DEFAULT,GZIP,BZIP2,LZO,LZ4,SNAPPY  
    codec = compressionCodecs.getCodec(file);   
  
    // open the file and seek to the start of the split  
    FileSystem fs = file.getFileSystem(job);  
    FSDataInputStream fileIn = fs.open(split.getPath());  
  
    if (isCompressedInput()) {  
    //通过CodecPool的getCompressor方法获得Compressor对象，该方法需要传入一个codec，  
    //然后Compressor对象在createOutputStream中使用，使用完毕后再通过returnCompressor放回去  
      decompressor = CodecPool.getDecompressor(codec);  
      if (codec instanceof SplittableCompressionCodec) {  
        final SplitCompressionInputStream cIn =  
          ((SplittableCompressionCodec)codec).createInputStream(  
            fileIn, decompressor, start, end,  
            SplittableCompressionCodec.READ_MODE.BYBLOCK);  
        in = new LineReader(cIn, job);  
        start = cIn.getAdjustedStart();  
        end = cIn.getAdjustedEnd();  
        filePosition = cIn;  
      } else {  
        in = new LineReader(codec.createInputStream(fileIn, decompressor),  
            job);  
        filePosition = fileIn;  
      }  
    } else {  
      fileIn.seek(start);  
      in = new LineReader(fileIn, job);  
      filePosition = fileIn;  
    }  
    // If this is not the first split, we always throw away first record  
    // because we always (except the last split) read one extra line in  
    // next() method.  
    if (start != 0) {  
      start += in.readLine(new Text(), 0, maxBytesToConsume(start));  
    }  
    this.pos = start;  
  }  
    
  private boolean isCompressedInput() {  
    return (codec != null);  
  }  
  
  private int maxBytesToConsume(long pos) {  
    return isCompressedInput()  
      ? Integer.MAX_VALUE  
      : (int) Math.min(Integer.MAX_VALUE, end - pos);  
  }  
  
  private long getFilePosition() throws IOException {  
    long retVal;  
    if (isCompressedInput() && null != filePosition) {  
      retVal = filePosition.getPos();  
    } else {  
      retVal = pos;  
    }  
    return retVal;  
  }  
  
//读取每一行数据的时候，都会执行nextKeyValue()方法。  
//返回为true的时候，就会再调用getCurrentKey和getCurrentValue方法获取，key，value值  
  public boolean nextKeyValue() throws IOException {  
    if (key == null) {  
      key = new LongWritable();  
    }  
    key.set(pos);  
    if (value == null) {  
      value = new Text();  
    }  
    int newSize = 0;  
    // We always read one extra line, which lies outside the upper  
    // split limit i.e. (end - 1)  
    while (getFilePosition() <= end) {  
    //在这里进行数据读取，LineReader以\n作为分隔符，读取一行数据，放到Text value里面  
     //读取一行，可以参考LineReader的源码实现  
      newSize = in.readLine(value, maxLineLength,
          Math.max(maxBytesToConsume(pos), maxLineLength));  
      if (newSize == 0) {  
        break;  
      } 
      
      
      if(isFilter&&filter(value.toString())) {
    	  return false;
      }
      
      
      pos += newSize;  
      if (newSize < maxLineLength) {  
        break;  
      }  
  
      // line too long. try again  
      LOG.info("Skipped line of size " + newSize + " at pos " +   
               (pos - newSize));  
    }  
    if (newSize == 0) {  
      key = null;  
      value = null;  
      return false;  
    } else {  
      return true;  
    }  
  }  
  
  @Override  
  public LongWritable getCurrentKey() {  
    return key;  
  }  
  
  @Override  
  public Text getCurrentValue() {  
	  //System.out.println("debug"+value);
	  value.set(className+","+value.toString());
    return value;  
  }  
  
  /** 
   * Get the progress within the split 
   */  
  public float getProgress() throws IOException {  
    if (start == end) {  
      return 0.0f;  
    } else {  
      return Math.min(1.0f,  
        (getFilePosition() - start) / (float)(end - start));  
    }  
  }  
  
  public synchronized void close() throws IOException {  
    try {  
      if (in != null) {  
        in.close();  
      }  
    } finally {  
      if (decompressor != null) {  
        CodecPool.returnDecompressor(decompressor);  
      }  
    }  
  }
  
  //huo qu lei ming
  private String getClassName(String str) {
	  String[] subStr=str.split("/");
	  return subStr[6];
  }
  
  //guo lv qi   guo lv hanyou  shuzi  de  String
  //if true ,drop out
  
  private boolean filter(String str) {
	  //TODO
	  System.out.println(str);
	  //Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
	  for(int i=0;i<str.length();i++) {
		  if(str.charAt(i)>='0'&&str.charAt(i)<='9') {
			  return true;
		  }
	  }
	  return false;
  }
}  