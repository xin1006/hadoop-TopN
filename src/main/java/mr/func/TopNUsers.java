package mr.func;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Extract top N users from the access log
 */
public class TopNUsers {

  /**
   * As the Map operation is parallelized the input file set is first split to several pieces.
   * <p>
   * Mapper is run for the the split - the input file is split into several splits (of size 64MB).
   * Mapper works with a record - each line from the split is a record
   */
  public static class Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, IntWritable> {
    private static final Logger LOGGER = LoggerFactory.getLogger(Mapper.class);

    private static final String USER_REGEX = "(username=')([A-Za-z0-9]{8}+)(')";
    private static final Pattern USER_PATTERN = Pattern.compile(USER_REGEX);
    private static final IntWritable one = new IntWritable(1);
    private Text usernameWord = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      Matcher matcher = USER_PATTERN.matcher(value.toString());
      if (matcher.find()) {
        String username = matcher.group(2);
        usernameWord.set(username);
        context.write(usernameWord, one);
      }
    }
  }

  /**
   * Combines partitions (key-value pairs that are output of mapper) into summed pairs key-num of actions.
   * <p>
   * IMPORTANT: this is done for mapper only, not for all partitions.
   * <p>
   * When the map operation outputs its pairs they are already available in memory.
   * <p>
   * If a combiner is used then the map key-value pairs are not immediately written to the output.
   * Instead they will be collected in lists, one list per each key value.
   */
  public static class Combiner extends org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable> {
    private static final Logger LOGGER = LoggerFactory.getLogger(Combiner.class);

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }
      context.write(key, new IntWritable(sum));
    }
  }

  /**
   * Combines partitions globally, for all mappers.
   */
  public static class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable> {
    private static final Logger LOGGER = LoggerFactory.getLogger(Reducer.class);

    Map<String, Integer> counters = new ConcurrentHashMap<String, Integer>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }
      counters.put(key.toString(), sum);
      LOGGER.debug("USERNAME: {}, COUNT: {}", key.toString(), sum);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      // sort the users by counters
      List<String> keys = new ArrayList<String>(counters.keySet());
      try {
        keys
          .stream()
          .sorted((k1, k2) -> counters.get(k2).compareTo(counters.get(k1)))
          .limit(10)
          .collect(Collectors.toList())
          .forEach(k -> {
            try {
              context.write(new Text(k), new IntWritable(counters.get(k)));
            } catch (Exception ex) {
              throw new RuntimeException(ex);
            }
          });
      } catch (RuntimeException ex) {
        if (ex.getCause() != null) {
          if (ex.getCause() instanceof IOException) {
            throw (IOException)ex.getCause();
          } else if (ex.getCause() instanceof InterruptedException) {
            throw (InterruptedException)ex.getCause();
          }
        } else {
          throw ex;
        }
      }
//      Collections.sort(keys, new Comparator<String>() {
//        @Override
//        public int compare(String o1, String o2) {
//          return counters.get(o2).compareTo(counters.get(o1));
//        }
//      });
//      int i = 0;
//      for (String key : keys) {
//        if (i++ == 10) {
//          break;
//        }
//        context.write(new Text(key), new IntWritable(counters.get(key)));
//      }
    }
  }
}
