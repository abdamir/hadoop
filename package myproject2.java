package myproject2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperature {
	public static class MaxTemperatureMapper
	  extends Mapper<LongWritable, Text, Text, IntWritable> {

	  private static final int MISSING = 9999;
	  
	  @Override
	  public void map(LongWritable key, Text value, Context context)
	      throws IOException, InterruptedException {
	    //fetching relevant information from in form of substrings
		//dataset used had no partition/separation key
		//one method was to make changes in dataset by setting partition key
		//if we look at data the string length and its format it is consistent so we can fetch substrings
		//by mentioning indexes
	    String line = value.toString();
	    String year = line.substring(15, 19);//fetching year
	    int airTemperature;
	  //fetching air temperature
	    if (line.charAt(87) == '+') { // parseInt can not handle other characters
	      airTemperature = Integer.parseInt(line.substring(88, 92));
	    } else {
	      airTemperature = Integer.parseInt(line.substring(87, 92));
	    }
	    
	    String quality = line.substring(92, 93);
	    if (airTemperature != MISSING && quality.matches("[01459]")) {
	      context.write(new Text(year), new IntWritable(airTemperature));
	    }
	  }
	}
	
	public static class MaxTemperatureReducer
	  extends Reducer<Text, IntWritable, Text, IntWritable> {
	  
	  @Override
	  public void reduce(Text key, Iterable<IntWritable> values,
	      Context context)
	      throws IOException, InterruptedException {
	    
	    int maxValue = Integer.MIN_VALUE;
	    for (IntWritable value : values) {
	      maxValue = Math.max(maxValue, value.get());//getting the max temp value
	    }
	    context.write(key, new IntWritable(maxValue/10));
	  }
	}

public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperature <input path> <output path>");
      System.exit(-1);
    }
    //creating a new config
    Configuration conf = new Configuration();
    //creating new job using that config
    Job job = Job.getInstance(conf, "maxtemp");//giving maxtemp name to job we created
    job.setJarByClass(MaxTemperature.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));//setting input path from arguments
    FileOutputFormat.setOutputPath(job, new Path(args[1]));//setting output path from arguments
    
    job.setMapperClass(MaxTemperatureMapper.class); //setting mapper class
    job.setReducerClass(MaxTemperatureReducer.class);//setting reducer class

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
  
}