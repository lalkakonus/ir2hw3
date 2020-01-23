import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;


// Среднее время работы с запросом
public class TAvgWorkTimeQuery extends BaseStatistic {
    public TAvgWorkTimeQuery(HashMap<String, Integer> URLmap, HashMap<String, Integer> queryMap){
        this.URLMap = URLmap;
        this.QueryMap = queryMap;
        this.KeyPrefix = Config.AVG_WORK_TIME_QUERY_KEY;
    }

    public TAvgWorkTimeQuery(){
        this.URLMap = new HashMap<>();
        this.QueryMap = new HashMap<>();
        this.KeyPrefix = Config.AVG_WORK_TIME_QUERY_KEY;
    }

    @Override
    public void map(TDataConverter.TResult Serp, Mapper.Context context, boolean hostFlag) throws IOException, InterruptedException {
        if (hostFlag) {
            return;
        }
        if (Serp.Clicked.size() > 1) {
            long diff = Serp.Clicked.get(Serp.Clicked.size() - 1).Timestamp - Serp.Clicked.get(0).Timestamp;
            Text value = new Text(Long.toString(diff));
            write(context, hostFlag, Config.EMPTY, Serp.Query, value);
        }
    }

    @Override
    public void reduce(Text word, Iterable<Text> nums, Reducer.Context context) throws IOException, InterruptedException {
        long sum_time = 0;
        int show_cnt = 0;
        for (Text i : nums) {
            sum_time += Long.parseLong(i.toString());
            ++show_cnt;
        }
        context.write(word, new Text(Float.toString((float) sum_time / show_cnt)));
    }
}
