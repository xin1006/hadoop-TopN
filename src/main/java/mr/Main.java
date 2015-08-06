package mr;

import mr.func.TopNUsers;
import mr.func.WordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;

public class Main {
  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

  public static void main(String... args) throws Exception {
    final String[] arguments = args;
    UserGroupInformation userGroupInformation = UserGroupInformation.createRemoteUser("zedar");
    userGroupInformation.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        Configuration conf = new Configuration();
        conf.set("hadoop.job.ugi", "zedar");
        Job job = Job.getInstance(conf, "TOP N USERS");

        job.setJarByClass(Main.class);

        job.setMapperClass(TopNUsers.Mapper.class);
        job.setCombinerClass(TopNUsers.Combiner.class);
        job.setReducerClass(TopNUsers.Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path input = null, output = null;
        boolean localFS = false;
        if (arguments.length >= 2) {
          // run in local mode - no server is running, either name or data, or yarn nodes are running
          LOGGER.debug("In local hadoop mode");
          input = new Path(arguments[0]);
          output = new Path(arguments[1]);
          localFS = true;
        } else {
          // run in server mode
          LOGGER.debug("In server hadoop mode");
          conf.set("yarn.resourcemanager.address", "localhost:8032");
          conf.set("yarn.resourcemanager.scheduler.address", "localhost:8030");
          conf.set("mapreduce.framework.name", "local");
          conf.set("fs.default.name", "hdfs://localhost:54310");

          input = new Path("hdfs://localhost:54310/user/zedar/input");
          output = new Path("hdfs://localhost:54310/user/zedar/output");
        }

        LOGGER.debug("Input filename URI: {}", input.toUri().toString());
        LOGGER.debug("Output filename URI: {}", output.toUri().toString());

        FileSystem fs = FileSystem.get(conf);
        fs.delete(output, true);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        // ---------------------------------------------
        if (job.waitForCompletion(true)) {
          LOGGER.debug("JOB EXECUTION: SUCCEEDED");
          FileStatus[] status = fs.listStatus(new Path(localFS ? output.toUri().toString() : output.getName()), new PathFilter() {
            @Override
            public boolean accept(Path path) {
              return path.getName().startsWith("part");
            }
          });

          for (int i = 0; i < status.length; i++) {
            FSDataInputStream is = fs.open(status[i].getPath());
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            String line = br.readLine();
            while(line != null) {
              LOGGER.debug("RESULT: {}", line);
              line = br.readLine();
            }
            is.close();
          }
        } else {
          LOGGER.debug("JOB EXECUTION FAILED");
        }

        return null;
      }
    });
  }
}
