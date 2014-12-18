package cmpt741;

import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.Map;
import java.util.HashSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.util.*; 
//import org.apache.hadoop.mapreduce.JobContext;
//import org.apache.hadoop.mapreduce.MapContext;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.*;

public class Project 
{
	
	/************************************************************************
	* First Mapper Class
	*************************************************************************/
	public static class Map1 extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> 
	{
		// IntWritable for values
		private final static IntWritable one = new IntWritable(1);
		// Text class for keys
		private Text freq_itemset = new Text();
		
		private double minsupport;

		public void configure(JobConf job) 
		{
			minsupport = (job.getDouble("minsupport", 0));
       		}
				
		// Map function format : (key , value ,output of mapper format,reporter)
		public void map (LongWritable key, Text value, OutputCollector<Text,IntWritable> output, Reporter reporter) throws IOException 
		{
			String baskets = value.toString();
			
			Apriori a = new Apriori(minsupport);
			Itemsets patterns = a.runApriori(baskets);
			
			for (List<Itemset> level : patterns.getLevel()) 
			{
				for (Itemset itemset : level) 
				{
					//Arrays.sort(itemset.itemset);
					//reporter.progress();
					freq_itemset.set(Arrays.toString(itemset.getItems()));
					output.collect(freq_itemset, one);
				}
			}
		}
	}


	/************************************************************************
	* First Reducer Class
	*************************************************************************/
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> 
	{
	    private final static IntWritable one = new IntWritable(1);
	    
		public void reduce (Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException 
		{
			output.collect(key, one);
		}
	}

	
	/************************************************************************
	* Second Mapper Class
	*************************************************************************/
	public static class Map2 extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> 
	{
		// IntWritable for values
		private IntWritable support = new IntWritable();
		
		// Text class for keys
		private Text freq_itemset = new Text();

		private Path[] myCacheFiles;
		private List<String> str = null;
		private List<Integer[]> mainarray =null;

		public void configure(JobConf job)// throws IOException, InterruptedException 
		{
			try 
			{
                       System.err.println("start configure 1");
         			myCacheFiles = DistributedCache.getLocalCacheFiles(job);//.getConfiguration());
				str = FileUtils.readLines(new File(myCacheFiles[0]+"/part-00000"));
				String cands[] = str.toArray(new String[str.size()]);//.split("\r?\n|\r");
                           mainarray = new ArrayList<Integer[]>();
				Integer[] results =null;
				String[] items = null;
                      System.err.println("start configure 2");
			for (String candidate : cands) 
			{
				

				String candidateSplited = candidate.split("\t")[0];
				 items = candidateSplited.replaceAll("\\[", "")
					.replaceAll("\\]", "").split(", ");
				
				 results = new Integer[items.length];
                            System.err.println("start configure 3");
				for (int i = 0; i < items.length; i++) 
				{
					results[i] = Integer.parseInt(items[i]);
				}
                               mainarray.add(results);
                           System.err.println("end configure 3");
                        }
                        System.err.println("end configure 1");
			}
			catch (Exception e)
			{
				System.out.println("File error: ");
				e.printStackTrace();
			}
       		}

		// Map function format : (key , value ,output of mapper format,reporter)
		public void map (LongWritable key, Text value, OutputCollector<Text,IntWritable> output, Reporter reporter) throws IOException 
		{
			reporter.setStatus("HELLLOOOOO");
		System.err.println("start map");
			Map<Integer[], Integer> mapItemCount = new HashMap<Integer[], Integer>();
			List<Integer[]> mainouterarray = new ArrayList<Integer[]>();
			String trans[] = value.toString().split("\r?&|\r");
			//String trans = value.toString();
			int count = 0;
                    System.err.println("start outer");
					 for (String transaction : trans) 
				{
                        
					String[] it = transaction.split(" ");
					Integer outer[] = new Integer[it.length];
					for (int i = 0; i < it.length; i++) 
					{
						Integer item = Integer.parseInt(it[i]);
						outer[i] = item;
					}
                      mainouterarray.add(outer);
                            }          
                               
                   System.err.println("end outer");

			for (Integer[] results : mainarray) 
			{
				count = 0;
				
				for(Integer[] outer : mainouterarray)
				{
                             HashSet s1 =new HashSet(Arrays.asList(outer));
                            HashSet s2 =new HashSet(Arrays.asList(results));
                               System.err.println("start compare");

					if (s1.size() >= s2.size())
					{
						if (s1.containsAll(s2)) 
						{
							count++;
						}
					}
				}
				mapItemCount.put(results, count);
                                System.err.println("end compare");

			}

			List<Integer[]> keys = new ArrayList<Integer[]>(mapItemCount.keySet());
                       System.err.println("start iterate");
			for (Integer[] cand_array : keys) 
			{
				freq_itemset.set(Arrays.toString(cand_array));
                                support.set(mapItemCount.get(cand_array));

				output.collect(freq_itemset, support);
			}
System.err.println("end iterate");
System.err.println("end map");
		}
	}
	
	/*public static boolean linearIn(Integer[] transaction, Integer[] candidate) 
	{
		return Arrays.asList(transaction).containsAll(Arrays.asList(candidate));
	}
         */
	/************************************************************************
	* Second Reducer Class
	*************************************************************************/
	public static class Reduce2 extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> 
	{
	    private final static IntWritable one = new IntWritable(1);
	    
		private int minsupport_absolute;

		public void configure(JobConf job) 
		{
			minsupport_absolute = (job.getInt("minsupport_absolute", 0));
       		}

		public void reduce (Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException 
		{
			int sup=0;
			int baskets = 0;
                   
			while (values.hasNext()) 
			{
				
                                sup += values.next().get();
			}
			if( sup >= minsupport_absolute)
			{
				output.collect(key, new IntWritable(sup));
			}

		}
	}
	
	static class ValueComparator implements Comparator<String> 
	{

	    Map<String, Integer> base;
	    public ValueComparator(Map<String, Integer> base) 
	    {
		this.base = base;
	    }

	    // Note: this comparator imposes orderings that are inconsistent with equals.    
	    public int compare(String a, String b) 
	    {
		if (base.get(a) > base.get(b)) 
		{
			return -1;
		} else if (base.get(a).compareTo(base.get(b))== 0)
		{
				if (a.compareTo(b) > 0)
					return 1;
				else
					return -1;
		} else
		{
			return 1;
		} // returning 0 would merge keys
	    }
	}

	
	public static class NLinesInputFormat extends TextInputFormat  
{  
   @Override
   public RecordReader<LongWritable, Text> getRecordReader(InputSplit split, JobConf conf, Reporter reporter)throws IOException     {   
       // reporter.setStatus(split.toString());  
        return new NLineRecordReader(conf, (FileSplit)split);
    }
}



public static class NLineRecordReader implements RecordReader<LongWritable, Text> 
{
        private LineRecordReader lineRecord;
        private LongWritable lineKey;
        private Text lineValue;
        public NLineRecordReader(JobConf conf, FileSplit split) throws IOException {
            lineRecord = new LineRecordReader(conf, split);
            lineKey = lineRecord.createKey();
            lineValue = lineRecord.createValue();
        }

        @Override
        public void close() throws IOException {
            lineRecord.close();
        }

        @Override
        public LongWritable createKey() {
            return new LongWritable();

        }

        @Override
        public Text createValue() {
            return new Text("");

        }

        @Override
        public float getProgress() throws IOException {
            return lineRecord.getPos();

        }

        @Override
        public synchronized boolean next(LongWritable key, Text value) throws IOException {
            boolean appended, gotsomething;
            boolean retval;
			//int count=0;
            byte space[] = {'&'};
            value.clear();
            gotsomething = false;
            do {
                appended = false;
                retval = lineRecord.next(lineKey, lineValue);
                if (retval) {
				//    count++;
                    if (lineValue.toString().length() > 0) {
                        byte[] rawline = lineValue.getBytes();
                        int rawlinelen = lineValue.getLength();
                        value.append(rawline, 0, rawlinelen);
                        value.append(space, 0, 1);
                        appended = true;
                    }
                    gotsomething = true;
                }
            } while (appended);

            //System.out.println("ParagraphRecordReader::next() returns "+gotsomething+" after setting value to: ["+value.toString()+"]");
            return gotsomething;
        }

        @Override
        public long getPos() throws IOException {
            return lineRecord.getPos();
        }
    }  
	
	
	
	public static void printResults(String path)
	{
		PrintWriter writer = null;
		try
		{
			writer = new PrintWriter("final_output.txt", "UTF-8");


                        Path pt=new Path(path+"/part-00000");
                        FileSystem fs = FileSystem.get(new Configuration());
                        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
                        List<String> str = new ArrayList<String>();
                        String line=br.readLine();

                        while (line != null)
			{
                                str.add(line);
                                line=br.readLine();
                        }

			Map<String, Integer> mapItemCount = new HashMap<String, Integer>();
			String cands[] = str.toArray(new String[str.size()]);
			
			String cand;
			int cand_support=0;
                        int total_frequentset = str.size();
		        ValueComparator cmp = new ValueComparator(mapItemCount);
			TreeMap<String,Integer> sorted_map = new TreeMap<String,Integer>(cmp);
				
			for (String candidate : cands) 
			{
				cand = candidate.split("\t")[0];
				cand_support = Integer.parseInt(candidate.split("\t")[1]);
				mapItemCount.put(cand, cand_support);
			}	
			
			sorted_map.putAll(mapItemCount);
                    	writer.println(total_frequentset+"\n");

			for(Map.Entry<String,Integer> entry : sorted_map.entrySet()) 
			{
 				String key = entry.getKey();
				Integer value = entry.getValue();

				writer.println(key + " (" + value + ")");
			}
		}
		catch (IOException e)
		{
			System.out.println("File error: ");
			e.printStackTrace();
		}
		finally
		{
			writer.close();
		}	
	}


  
    	/************************************************************************
	* Main function
	*************************************************************************/
	public static void main(String[] args) throws Exception 
	{
		// args are as follows: 
		// 0: input directory path
		// 1: intermediate directory path
		// 2: output directory path
		// 3: support in %
		// 4: support in absolute number
		// 5: k = number of map tasks
		 
		// First Job
		// Configuration of jobs
		JobConf conf = new JobConf(Project.class);
		conf.setJobName("pass1");
		conf.setNumMapTasks(Integer.parseInt(args[5]));
		conf.setNumReduceTasks(1);
		long milliSeconds = 600*60*60;
		conf.setLong("mapreduce.task.timeout", milliSeconds);
		
		// Key and Value format defined in Map and Reduce class 
		conf.setOutputKeyClass(Text.class); 
		conf.setOutputValueClass(IntWritable.class);
		
		// Set Mapper , Combiner , Reducer 
		conf.setMapperClass(Map1.class); 
		//conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		// Input and Output format of the task 
		conf.setInputFormat(NLinesInputFormat.class); 
		conf.setOutputFormat(TextOutputFormat.class);
		
		// Reading hdfs path (input, output) from args 
		FileInputFormat.setInputPaths(conf, new Path(args[0])); 
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		// set support parameter
		conf.setDouble("minsupport", Double.parseDouble(args[3]));

		// RUN !!!!!
		JobClient.runJob(conf);
		
		//-----------------------------------------------------
		// Second Job
		// Configuration of jobs
//long milliSeconds = 600*60*60;
		JobConf conf2 = new JobConf(Project.class);
		conf2.setJobName("pass2");
		conf2.setNumMapTasks(Integer.parseInt(args[5]));
		conf2.setNumReduceTasks(1);
		conf2.setLong("mapreduce.task.timeout", milliSeconds);

		// Key and Value format defined in Map and Reduce class 
		conf2.setOutputKeyClass(Text.class); 
		conf2.setOutputValueClass(IntWritable.class);
		
		// Set Mapper , Combiner , Reducer 
		conf2.setMapperClass(Map2.class); 
		//conf.setCombinerClass(Reduce.class);
		conf2.setReducerClass(Reduce2.class);

		// Input and Output format of the task 
		//conf2.setInputFormat(TextInputFormat.class); 
		conf2.setInputFormat(NLinesInputFormat.class);
		conf2.setOutputFormat(TextOutputFormat.class);
		
		// Reading hdfs path (input, output) from args 
		FileInputFormat.setInputPaths(conf2, new Path(args[0])); 
		FileOutputFormat.setOutputPath(conf2, new Path(args[2]));

		// Set intermediate candidate file
		String candidateFile = args[1];
		//conf2.addCacheFile(new URI(candidateFile + "#part-00000")); 
		DistributedCache.addCacheFile(new Path(candidateFile).toUri(), conf2);
		//conf2.setStrings("path", candidateFile);
		// set support parameter
		conf2.setInt("minsupport_absolute", Integer.parseInt(args[4]));

		// RUN !!!!!
		JobClient.runJob(conf2);

		//-----------------------------------------------------
		// Print final output
		printResults(args[2]);
		
	}
}
