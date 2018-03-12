package mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
//import org.mockito.cglib.core.classTermNumKey;

import format.MyInputFormatInClass;
import format.PriorProInputFormat;

public class Train {

	private static Map<String, Integer> classTermNum = new HashMap<String,Integer>();//每个类文档中单词频率
	private static Map<String, Integer> classDocNum = new HashMap<String,Integer>();//每个类的文档数量
	private static Map<String, Double> classPro=new HashMap<String,Double>();//每个类的先验概率
	private static List<String> dictionary=new ArrayList<String>(); //字典
	private static int docNum=0; //记录文档总数

	
	//wordcount的reduce函数，在这里作为combiner使用
	public static class Combiner extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	
	//统计每个类中的term频率的map，
	//输出 <<class，term>，1>
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String token = itr.nextToken();

				word.set(token);
				context.write(word, one); 
				
				//key的格式为 class,term ，所以这里调用split获取term
				String[] strs = token.split(",");
				
				if(!dictionary.contains(strs[1])) {       
					dictionary.add(strs[1]);       //创建辞典
				}
			}
		}
	}

	//统计每个类中的term频率的reduce
	//输入  <<class，term>，{1,1,1...}>
	//输出  <<class，term>，num> ,同时把结果保存在classTermNum中
	public static class IntSumInClassReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable one = new IntWritable(1);
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			//<class,term>  实际格式为 class,term  调用split获取class和term
			String[] strs = key.toString().split(",");
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			String className = strs[0];
			if (!classTermNum.containsKey(className)) {//计算每个类中每个term出现频率
				classTermNum.put(className, sum);
			} else {
				classTermNum.put(className, classTermNum.get(className) + sum);
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	//计算每个类的条件概率
	//输出<<class,term>,num>
	public static class CalculateProMapper extends Mapper<Object, Text, Text, IntWritable> {

		//private final static IntWritable one = new IntWritable(1);
		private final static IntWritable count = new IntWritable();
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String str=itr.nextToken();
				//因为class term的分割符为逗号  而<class，term> num的分割符也为逗号
				//所以实际的格式为class,term,num  这里调用split获取每个字符串
				String[] strs = str.split(",");
				if (strs.length == 3) {
					word.set(strs[0] + "," + strs[1]);
					count.set(Integer.parseInt(strs[2]));
					context.write(word, count);
				}
			}
		}
	}

	//输出<<class,term>,概率>
	public static class CalculateProReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
		//private IntWritable result = new IntWritable();
		private DoubleWritable result = new DoubleWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			
			String[] strs = key.toString().split(",");
			String className=strs[0];
			int value=0;
			for (IntWritable val : values) { //这里只循环一次
				value=val.get();
			}
			double pro = (value+1.0) / (classTermNum.get(className) + dictionary.size());
			result.set(pro);
			context.write(key, result);
		}
	}
	
	//计算先验概率
	//输出 <class,1>
	public static class PriorProMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	//计算先验概率
	//输入 <class,{1,1...}>
	//输出 <class,num>  ，同时存入classDocNum中
	public static class PriorProReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			
			docNum+=sum;  //统计文档总数
			classDocNum.put(key.toString(),sum); //记录每个类别文档数量
			
			result.set(sum);
			context.write(key, result);
		}
	}
	
	
	
	//通过主目录便利获取每个类的文件目录， 用逗号分隔， 作为job1的输入
	private static String getInputFilePath(String path) throws Exception{
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(URI.create(path), conf);
		FileStatus[] fs = hdfs.listStatus(new Path(path));
		Path[] listPaths = FileUtil.stat2Paths(fs);
		StringBuffer buff = new StringBuffer();
		for (int i = 0; i < listPaths.length - 1; i++) {
			buff.append(listPaths[i].toUri().toString());
			buff.append(",");
		}
		buff.append(listPaths[listPaths.length - 1].toUri().toString());
		return buff.toString();
	}
	
	//通过统计结果计算先验概率
	private static void calculateClassPro() throws IOException {
		double pro;
		for (Map.Entry<String, Integer> entry : classDocNum.entrySet()) { 
			pro=entry.getValue()/((double)docNum);
			classPro.put(entry.getKey(), pro);
		}
	}
	

	
	//创建文件并写入
	private static void writeClassPro(String resultPath) throws IOException {
		Configuration conf = new Configuration();  
        FileSystem fs = FileSystem.get(conf);  
        Path path = new Path(resultPath+"/classPro");  
        FSDataOutputStream out = fs.create(path);  
        String inLine;
      //写入每个类先验概率
        for (Map.Entry<String, Double> entry : classPro.entrySet()) { 
			inLine=entry.getKey()+","+entry.getValue()+"\n";
			out.writeBytes(inLine);
		}
        
        double pro;
      //写入每个类中未出现词的概率，因为所有的都一样，所以在这里记录一次，节省内存，提高读取效率
        for (Entry<String, Integer> entry : classTermNum.entrySet()) { 
        	pro=1.0/(entry.getValue()+dictionary.size());
        	//System.out.println(entry.getValue()+"  "+dictionary.size());
			inLine=entry.getKey()+"default"+","+pro+"\n";
			out.writeBytes(inLine);
		}
        
        fs.close();
	}
	
	//打印结果到控制台，调试用
	public static void printResult() {
		for (Map.Entry<String, Integer> entry : classTermNum.entrySet()) {

			System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());

		}
		System.out.println("the size of dictionary is " + dictionary.size());
		
	}
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		//设置输出分隔符为，
		//这里之所以设为逗号，是因为class term 分隔符为逗号，所有为了统一，将key value也设为逗号
		conf.set("mapred.textoutputformat.separator", ",");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: wordcount <in> <out_tmp> <out>");
			System.exit(2);
		}
		
		String inputPath=getInputFilePath(args[0]);
		System.out.println(inputPath);
		
		//job1 读取文件，统计每个类的文档单词出现频率
		Job job1 = new Job(conf, "job1");
		job1.setJarByClass(Train.class);
		job1.setMapperClass(TokenizerMapper.class);
		job1.setCombinerClass(Combiner.class);
		job1.setReducerClass(IntSumInClassReducer.class);

		//自定义job1的输入类型
		job1.setInputFormatClass(MyInputFormatInClass.class); 

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job1, inputPath);
		FileOutputFormat.setOutputPath(job1, new Path("output_tmp"));

		//计算每个类的每个单词在该类中的出现频率
		Job job2 = new Job(conf, "job2");
		job2.setMapperClass(CalculateProMapper.class);
		job2.setReducerClass(CalculateProReducer.class);

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job2, new Path("output_tmp"));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));
		
		//统计每个类的文档数量
		Job job3 = new Job(conf, "job3");
		job3.setMapperClass(PriorProMapper.class);
		job3.setCombinerClass(Combiner.class);
		job3.setReducerClass(PriorProReducer.class);
		job3.setInputFormatClass(PriorProInputFormat.class);//自定义输入类型，不读取文件，仅读取文件类别名

		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job3, inputPath);
		FileOutputFormat.setOutputPath(job3, new Path(otherArgs[2]));
		
		
		if (job3.waitForCompletion(true)) {//先统计先验概率，再统计条件概率
			if (job1.waitForCompletion(true)) {//job1的输出作为job2的输入，所以job1完成后，job2再运行
				if (job2.waitForCompletion(true)) {
					calculateClassPro();
					printResult();
					writeClassPro(otherArgs[2]);
				}
				System.exit(job2.waitForCompletion(true) ? 0 : 1);
			}
		}
	}
}
