import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NGram {

  public static class NgramMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text Ngram = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      
      Configuration conf = context.getConfiguration();
      int nval = Integer.parseInt(conf.get("numResults"));

      String valueStr = value.toString();
      String[] parts = valueStr.split(" ");
      for(int i = 0; i < parts.length - nval + 1; i++){
        StringBuilder sb = new StringBuilder();
        for(int j = 0; j < nval; j++){
            if(j > 0) sb.append(' ');
            sb.append(parts[i+j]);
        }
        Ngram.set(sb.toString());
        context.write(Ngram, one);
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

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String temp_path = "temp";
    conf.set("numResults", args[2]);

    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(NGram.class);
    job.setMapperClass(NgramMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
}
