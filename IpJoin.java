import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class IpJoin {
	public static class IctMapper extends Mapper<LongWritable, Text, Text, Text> 
	{
		private String countries, years, prices,units;
		@Override
		public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString();
			String arr[] = line.split(",");
			countries = arr[4].trim();
			years = arr[3].trim();
			prices = arr[5].trim();
			units = arr[0].trim();
				if(!prices.equals("-"))
				{
					context.write(new Text(years), new Text(countries+","+prices+","+units));
				}
		}
	}
	public static class PassMapper extends Mapper<Object, Text, Text, Text> 
	{
		private String Years, Direction, Population;
		@Override
		public void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
				
			String line = value.toString();
			String arr[] = line.split(",");
			Years = arr[1].substring(0,4).trim();
			//int y= Integer.parseInt(Years);
			Direction = arr[3].trim();
			Population = arr[2].trim();
				if(!Population.isEmpty() &&
						(Direction.equals("Inward")||Direction.equals("Outward")||Direction.equals("All directions"))) {
					context.write(new Text(Years),new Text(Direction+":"+Population));
				}
				
		}
	}
	public static class InnerJoinReducer1 extends Reducer<Text, Text, Text, Text> {
		private String country, unit, price, direction, population;
		private float pr=0;
		private int po=0;
	    @Override
	    public void reduce(Text key, Iterable <Text> values, Context context)
	    throws IOException, InterruptedException {
	      for (Text value : values) {
	        String cur = value.toString();
	        if (cur.contains(",")) {
	          String arr[] = cur.split(",");
	          if(arr[0].equals("European Union - 27 countries (from 2020)")){
	          country=(arr[0].substring(0,29));
	          //unit=(arr[1]);
	          price=arr[1];
	          pr+=Float.parseFloat(price); 
	        }
	        }
	        else {
	        	String a[] = cur.split(":"); 
	        	if(a[0].equals("All directions")) {
	        	direction=a[0];
	        	population=a[1];
	        	po+=Integer.parseInt(population);
	        	}
	        }
	      }
	      String str = String.format("%.2f %d", pr, po);
	      if(direction!=null && direction.equals("All directions")) 
	      context.write(key, new Text(country+","+str+","+direction));
	      pr=0;
	      po=0;
	    }
	}
	public static class InnerJoinReducer2 extends Reducer<Text, Text, Text, Text> {
		private String country, unit, price, direction, population;
		private float pr=0;
		private int po=0;
	    @Override
	    public void reduce(Text key, Iterable <Text> values, Context context)
	    throws IOException, InterruptedException {
	      for (Text value : values) {
	        String cur = value.toString();
	        if (cur.contains(",")) {
	          String arr[] = cur.split(",");
	          if(arr[0].equals("Ireland")&&(arr[2].equals("EMP"))) {
	          country=(arr[0]);
	          unit=(arr[2]);
	          price=arr[1];
	          pr+=Float.parseFloat(price); 
	          }
	        }
	        else {
	        	String a[] = cur.split(":");
	        	if(a[0].equals("Inward")) {
	        	direction=a[0];
	        	population=a[1];
	        	po+=Integer.parseInt(population);
				}
			}
	     }
	      String str = String.format("%.2f %d", pr, po);
	      if(direction!=null&&direction.equals("Inward"))
	      context.write(key, new Text(country+","+str+","+direction));
	      pr=0;
	      po=0;
	    }
	}
	public static class InnerJoinReducer3 extends Reducer<Text, Text, Text, Text> {
		private String country, unit, price, direction, population;
		private float pr=0;
		private int po=0;
	    @Override
	    public void reduce(Text key, Iterable <Text> values, Context context)
	    throws IOException, InterruptedException {
	      for (Text value : values) {
	        String cur = value.toString();
	        if (cur.contains(",")) {
	          String arr[] = cur.split(",");
	          if(arr[0].equals("United Kingdom")&&(arr[2].equals("GVA"))) {
	          country=(arr[0]);
	          unit=(arr[2]);
	          price=arr[1];
	          pr+=Float.parseFloat(price); 
	          }
	        }
	        else {
	        	String a[] = cur.split(":");
	        	if(a[0].equals("Outward")) {
	        	direction=a[0];
	        	population=a[1];
	        	po+=Integer.parseInt(population);
				}
			}
		  }
	      String str = String.format("%.2f %d", pr, po);
	      if(direction!=null&&direction.equals("Outward"))
	      context.write(key, new Text(country+","+str+","+direction));
	      pr=0;
	      po=0;
	    }
	}
	public static void main( String[] args ) throws IOException, InterruptedException, ClassNotFoundException
    {
    	Configuration conf = new Configuration();
    	Job job = Job.getInstance(conf, "EconomicGrowth");
    	job.setJarByClass(IpJoin.class);
    	job.setJobName("Growth");
    	MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, IctMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PassMapper.class);
    	FileOutputFormat.setOutputPath(job, new Path(args[2]));
    	job.setReducerClass(InnerJoinReducer3.class);
    	job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
		outputPath.getFileSystem(conf).delete(outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
    }
}
