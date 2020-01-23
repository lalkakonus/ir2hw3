import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;


// средняя позиция документа, по которому кликают первым
public class TAvgFirstPositionQuery extends BaseStatistic {
    public TAvgFirstPositionQuery(HashMap<String, Integer> URLmap, HashMap<String, Integer> queryMap){
        this.URLMap = URLmap;
        this.QueryMap = queryMap;
        this.KeyPrefix = Config.AVG_FIRST_POSITION_QUERY_KEY;
    }

    public TAvgFirstPositionQuery(){
        this.URLMap = new HashMap<>();
        this.QueryMap = new HashMap<>();
        this.KeyPrefix = Config.AVG_FIRST_POSITION_QUERY_KEY;
    }

    @Override
    public void map(TDataConverter.TResult Serp, Mapper.Context context, boolean hostFlag) throws IOException, InterruptedException {
        if (hostFlag) {
            return;
        }
        if (!Serp.Clicked.isEmpty()) {
            Text value = new Text(Integer.toString(Serp.Clicked.get(0).Position));
            write(context, hostFlag, Config.EMPTY, Serp.Query, value);
        }
    }

    @Override
    public void reduce(Text word, Iterable<Text> nums, Reducer.Context context) throws IOException, InterruptedException {
        long sum_pos = 0;
        int show_with_click_cnt = 0;
        for (Text i : nums) {
            sum_pos += Integer.parseInt(i.toString());
            ++show_with_click_cnt;
        }
        context.write(word, new Text(Float.toString((float) sum_pos / show_with_click_cnt)));
    }
}
