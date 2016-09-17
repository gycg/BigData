import java.io.IOException;
import java.util.*;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopKWordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  static <K,V extends Comparable<? super V>>
  SortedSet<Map.Entry<K,V>> entriesSortedByValues(Map<K,V> map) {
    SortedSet<Map.Entry<K,V>> sortedEntries = new TreeSet<Map.Entry<K,V>>(
      new Comparator<Map.Entry<K,V>>() {
        @Override public int compare(Map.Entry<K,V> e1, Map.Entry<K,V> e2) {
          int res = e2.getValue().compareTo(e1.getValue());
          return res != 0 ? res : 1;
        }
      }
    );
    sortedEntries.addAll(map.entrySet());
    return sortedEntries;
  }

  // Create your 2nd mapper here
  public static class IdMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text word_freq = new Text();
    private Text k = new Text("-");
    //Map<String, Integer> map = new TreeMap<String, Integer>();
    //SortedSet<Map.Entry<String, Integer>> mapSorted = new TreeSet<Map.Entry<String, Integer>>();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        
        Scanner san = new Scanner(value.toString());
        //while(san.hasNext()) {
        //    map.put(san.next(), san.nextInt());
        //}
        //mapSorted = entriesSortedByValues(map);
        //System.out.println(mapSorted);
        while (san.hasNextLine()) {
          word_freq.set(san.nextLine());
          context.write(k, word_freq);
        }
    }
  }

  // Create your 2nd reducer here
  public static class IntMaxReducer
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();
    private String max_word;
    Map<String, Integer> map = new TreeMap<String, Integer>();
    SortedSet<Map.Entry<String, Integer>> mapSorted = new TreeSet<Map.Entry<String, Integer>>();
    int count = 0;

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
        
        Configuration conf = context.getConfiguration();
        String param = conf.get("numResults");
        for(Text val: values) {
            Scanner san = new Scanner(val.toString());
            map.put(san.next(), san.nextInt());
        }
        mapSorted = entriesSortedByValues(map);
        for (Map.Entry<String,Integer> element: mapSorted){
              String key1 = element.getKey();
              String value1 = Integer.toString(element.getValue());
            if(count<Integer.parseInt(param)){
                System.out.println(element);
                result.set(key1+"\t"+value1);
                context.write(key, result);
                count += 1;   
            }
        }
        //Scanner san = new Scanner(values.toString());
        //System.out.println(values);
        //while(san.hasNext()) {
        //    map.put(san.next(), san.nextInt());
        //}
        //mapSorted = entriesSortedByValues(map);
        //System.out.println(map);
        
        //result.set(san.next());
        //context.write(key, result);
    }
  }  

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(TopKWordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.waitForCompletion(true);


    Configuration conf2 = new Configuration();
    conf2.set("numResults", args[3]);
    Job job2 = Job.getInstance(conf2, "word count");

    // Insert your Code Here
    job2.setJarByClass(TopKWordCount.class);
    job2.setMapperClass(IdMapper.class);
    //job2.setCombinerClass(IntMaxReducer.class);
    job2.setReducerClass(IntMaxReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job2, new Path(args[1]));
    FileOutputFormat.setOutputPath(job2, new Path(args[2]));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);

  }
}
