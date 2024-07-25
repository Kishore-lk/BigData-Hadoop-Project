import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IctData
{
	public static class IctMapper extends Mapper<LongWritable, Text, Text, FloatWritable> 
	{
		private String country_code, countries, years, prices;
		@Override
		public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FloatWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString();
			String arr[] = line.split(",");
			country_code = arr[2].trim();
			countries = arr[4].trim();
			years = arr[3].trim();
			prices = arr[5].trim();
				if(!prices.equals("-") && !prices.isEmpty() && countries.equals("Ireland")) {
					float p=Float.parseFloat(prices);
					context.write(new Text(years+ "  " +countries), new FloatWritable(p));
				}
		}
	}
	public static class IctReduce extends Reducer<Text, FloatWritable, Text, FloatWritable>
	{
		@Override
		public void reduce(Text key, Iterable < FloatWritable > values, Context context)
				  throws IOException, InterruptedException {
			float d=0;
			for (FloatWritable t: values)
			    	d+=t.get();
			context.write(key, new FloatWritable(d));
		}
	}
	public static void main( String[] args ) throws IOException, InterruptedException, ClassNotFoundException
    {
    	Configuration conf = new Configuration();
    	Job job = Job.getInstance(conf, "ICTDatamapper");
    	job.setJarByClass(IctData.class);
    	job.setJobName("ICT");
    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
    	job.setMapperClass(IctMapper.class);
    	job.setReducerClass(IctReduce.class);
    	job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}