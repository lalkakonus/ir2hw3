import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.Math;
import java.util.HashMap;


// Среднее время просмотра документа
public class TAvgViewTimeDoc extends BaseStatistic {
    public TAvgViewTimeDoc(HashMap<String, Integer> URLmap, HashMap<String, Integer> queryMap){
        this.URLMap = URLmap;
        this.QueryMap = queryMap;
        this.KeyPrefix = Config.AVG_VIEW_TIME_DOC_KEY;
    }

    public TAvgViewTimeDoc(){
        this.URLMap = new HashMap<>();
        this.QueryMap = new HashMap<>();
        this.KeyPrefix = Config.AVG_VIEW_TIME_DOC_KEY;
    }

    @Override
    public void map(TDataConverter.TResult Serp, Mapper.Context context, boolean hostFlag) throws IOException, InterruptedException {
        for (int i = 0; i < (Serp.Clicked.size() - 1); ++i) {
            long diff = Serp.Clicked.get(i + 1).Timestamp - Serp.Clicked.get(i).Timestamp;
            Text value = new Text(Long.toString(diff));
            write(context, hostFlag, Serp.Clicked.get(i).URL, Config.EMPTY, value);
        }
    }

    @Override
    public void reduce(Text word, Iterable<Text> nums, Reducer.Context context) throws IOException, InterruptedException {
        long sum_time = 0;
        int clicked_cnt = 0;
        for (Text value : nums) {
            sum_time += Math.log(1 + Long.parseLong(value.toString()));
            ++clicked_cnt;
        }
        context.write(word, new Text(Long.toString(sum_time / clicked_cnt)));
    }
}
