import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;


// Средняя позиция кликнутых документов по всем выдачам запроса
public class TAvgClickPositionQuery extends BaseStatistic {
    public TAvgClickPositionQuery() {
        this.KeyPrefix = Config.AVG_CLICK_POSITION_QUERY_KEY;
        this.URLMap = new HashMap<>();
        this.QueryMap = new HashMap<>();
    }

    public TAvgClickPositionQuery(HashMap<String, Integer> URLmap, HashMap<String, Integer> queryMap) {
        this.KeyPrefix = Config.AVG_CLICK_POSITION_QUERY_KEY;
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
        for (TDataConverter.TClickedLink link : Serp.Clicked) {
            Text value = new Text(Integer.toString(link.Position));
            write(context, hostFlag, Config.EMPTY, Serp.Query, value);
        }
    }

    @Override
    public void reduce(Text word, Iterable<Text> nums, Reducer.Context context) throws IOException, InterruptedException {
        int sum_position = 0;
        int clicked_cnt = 0;
        for (Text i : nums) {
            sum_position += Integer.parseInt(i.toString());
            ++clicked_cnt;
        }
        context.write(word, new Text(Float.toString((float) sum_position / clicked_cnt)));
    }
}