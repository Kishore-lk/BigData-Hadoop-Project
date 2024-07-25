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

public class Passenger
{
	public static class PassMapper extends Mapper<Object, Text, Text, IntWritable> 
	{
		private String Years, Direction, Population;
		@Override
		public void map(Object kishore, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
		
			String line = value.toString();
			String arr[] = line.split(",");
			Years = arr[1].substring(0,4).trim();
			Direction = arr[3].trim();
			Population = arr[2].trim();
				if(!Population.isEmpty() 
						&&
						(Direction.equals("Inward")||Direction.equals("Outward"))) {
					System.out.println(Direction+"_" +Population);
					int p=Integer.parseInt(Population);
					context.write(new Text(Years+" "+Direction),new IntWritable(p));
				}
				
		}
	}
	public static class PassReduce extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		//String country,year;
		@Override
		public void reduce(Text key, Iterable < IntWritable > values, Context context)
				  throws IOException, InterruptedException {
			int d=0;
			for (IntWritable t: values)
			    	d+=t.get();
			context.write(key, new IntWritable(d));
		}
	}
	public static void main( String[] args ) throws IOException, InterruptedException, ClassNotFoundException
    {
    	Configuration conf = new Configuration();
    	Job job = Job.getInstance(conf, "PassengerData");
    	job.setJarByClass(IctData.class);
    	job.setJobName("Passenger");
    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
    	job.setMapperClass(PassMapper.class);
    	job.setReducerClass(PassReduce.class);
    	job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.out.println("Task Completed");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
    }

}