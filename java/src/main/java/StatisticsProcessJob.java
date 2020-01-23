import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;


public class StatisticsProcessJob extends Configured implements Tool {
    public static class StatisticsProcessMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final HashMap<String, Integer> URLDict = TDataConverter.CreateMap(new Path(Config.URL_MAP_FILEPATH));
        private final HashMap<String, Integer> QueryDict = TDataConverter.CreateMap(new Path(Config.QUERY_MAP_FILEPATH));

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            TDataConverter.TResult urlData = TDataConverter.Convert(value.toString());
            TDataConverter.TResult hostData = TDataConverter.ConvertToHost(value.toString());
            try {
                for (Class statisticClass : Config.ClassMap.values()) {
                    Constructor constructor = statisticClass.getConstructor(HashMap.class, HashMap.class);
                    BaseStatistic instance = (BaseStatistic)  constructor.newInstance(URLDict, QueryDict);
                    instance.map(urlData, context, false);
                    instance.map(hostData, context, true);
                }
            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException error) {
                System.out.println("Error: " + error.getMessage());
            }
        }
    }

    public static class StatisticsProcessReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text word, Iterable<Text> nums, Context context) throws IOException, InterruptedException {
            int delimiterPos = word.toString().indexOf(Config.DELIMITER);
            String keyType = word.toString().substring(0, delimiterPos);
            try {
                Class statisticClass = Config.ClassMap.get(keyType);
                if (statisticClass == null && keyType.substring(0, Config.HOST_KEY_PREFIX.length()).equals(Config.HOST_KEY_PREFIX)) {
                    statisticClass = Config.ClassMap.get(keyType.substring(Config.HOST_KEY_PREFIX.length()));
                }
                if (statisticClass == null) {
                    System.out.println("Reducer not found for class " + keyType);
                    return;
                }
                BaseStatistic instance = (BaseStatistic) statisticClass.getConstructor().newInstance();
                instance.reduce(word, nums, context);
            } catch (NoSuchMethodException | InstantiationException | InvocationTargetException | IllegalAccessException error) {
                System.out.println("Error: " + error.getMessage());
            }
        }
    }

    private Job getJobConf(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(StatisticsProcessJob.class);
        job.setJobName(StatisticsProcessJob.class.getCanonicalName());

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(StatisticsProcessMapper.class);
        job.setNumReduceTasks(40);
        // job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(StatisticsProcessReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        FileSystem fs = FileSystem.get(getConf());
        if (!fs.exists(new Path(Config.QUERY_MAP_FILEPATH)) || !fs.exists(new Path(Config.URL_MAP_FILEPATH))) {
            throw new Exception("Config file absent");
        }
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }
        Job job = getJobConf(args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static public void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new StatisticsProcessJob(), args);
        System.exit(ret);
    }
}
