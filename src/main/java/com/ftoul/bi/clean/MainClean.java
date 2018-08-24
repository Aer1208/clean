package com.ftoul.bi.clean;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.ftoul.bi.clean.output.MyOutputFormat;
import com.ftoul.bi.clean.util.RowParser;


/**
 * 清洗主程序
 * @author xiaohf
 *
 */
public class MainClean extends Configured implements Tool{
	
	public static final Log log = LogFactory.getLog(MainClean.class);
	
	public static class MapJoin extends Mapper<LongWritable, Text, Text, NullWritable> {
		private RowParser rowParser;
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String xmlFile = context.getConfiguration().get("xmlFile");
			try {
				rowParser = new RowParser(xmlFile);
			} catch (Exception e) {
				e.printStackTrace();
			}
			super.setup(context);
		}



		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();

			String repay = rowParser.parseRow(line);
			context.write(new Text(repay.toString()), NullWritable.get());
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		
		if (args == null || args.length < 3) {
			log.info("Usage:ReduceJoinBorrower <xmlFile> <input...> <output>");
			System.exit(1);
		}
		
		int exitCode = ToolRunner.run(new MainClean(), args);
		
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {
		if (args == null || args.length < 3) {
			log.info("Usage:ReduceJoinBorrower <xmlFile> <input...> <output>");
			return 1;
		}
		
		String xmlFile = args[0];

		Configuration conf = getConf();
		conf.set("xmlFile", xmlFile);
		Job job = Job.getInstance(conf, "CleanMain");
		for (int i = 1; i < args.length - 1; i++) {
			Path inPath = new Path(args[i]);
			FileInputFormat.addInputPath(job, inPath);
		}

		job.setMapperClass(MapJoin.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setPartitionerClass(HashPartitioner.class);
		//job.setNumReduceTasks(1);

		job.setReducerClass(Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setOutputFormatClass(MyOutputFormat.class);

		Path outPath = new Path(args[args.length - 1]);

		FileOutputFormat.setOutputPath(job, outPath);

		job.setJarByClass(MainClean.class);

		boolean success = job.waitForCompletion(true);
		return success ?1 :0;
	}
}
