import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;


// Среднее число кликов для запроса
public class TAvgClickCountQuery extends BaseStatistic {
    public TAvgClickCountQuery() {
        this.KeyPrefix = Config.AVG_CLICK_COUNT_QUERY_KEY;
        this.URLMap = new HashMap<>();
        this.QueryMap = new HashMap<>();
    }

    public TAvgClickCountQuery(HashMap<String, Integer> URLmap, HashMap<String, Integer> queryMap) {
        this.KeyPrefix = Config.AVG_CLICK_COUNT_QUERY_KEY;
        this.URLMap = URLmap;
        this.QueryMap = queryMap;
    }

    @Override
    public void map(TDataConverter.TResult Serp,
                    Mapper.Context context,
                    boolean hostFlag) throws IOException, InterruptedException {
        if (hostFlag) {
            return;
        }
        Text value = new Text(Integer.toString(Serp.Clicked.size()));
        write(context, hostFlag, Config.EMPTY, Serp.Query, value);
    }

    @Override
    public void reduce(Text word, Iterable<Text> nums, Reducer.Context context) throws IOException, InterruptedException {
        int sum_clicked = 0;
        int show_cnt = 0;
        for (Text i : nums) {
            sum_clicked += Integer.parseInt(i.toString());
            ++show_cnt;
        }
        context.write(word, new Text(Float.toString((float) sum_clicked / show_cnt)));
    }
}