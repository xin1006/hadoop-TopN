package mr.func;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount {
  public static class Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, IntWritable> {
    private static final Logger LOGGER = LoggerFactory.getLogger(Mapper.class);
    private static final IntWritable one = new IntWritable(1);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      LOGGER.debug("TEXT TO PARSE: {}", value.toString());
      Text word = new Text();
      StringTokenizer st = new StringTokenizer(value.toString());
      while (st.hasMoreTokens()) {
        word.set(st.nextToken());
        context.write(word, one);
      }
    }
  }

  public static class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable> {
    private static final Logger LOGGER = LoggerFactory.getLogger(Reducer.class);

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      LOGGER.debug("COUNTED {} : {}", key, sum);
      context.write(key, new IntWritable(sum));
    }
  }
}
