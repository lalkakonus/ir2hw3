import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;


// Средний номер клика по документу в выдачах по конкретному запросу
public class TAvgClickPositionQueryDoc extends BaseStatistic {
    public TAvgClickPositionQueryDoc(HashMap<String, Integer> URLmap, HashMap<String, Integer> queryMap){
        this.URLMap = URLmap;
        this.QueryMap = queryMap;
        this.KeyPrefix = Config.AVG_CLICK_POSITION_QUERY_DOC_KEY;
    }

    public TAvgClickPositionQueryDoc(){
        this.URLMap = new HashMap<>();
        this.QueryMap = new HashMap<>();
        this.KeyPrefix = Config.AVG_CLICK_POSITION_QUERY_DOC_KEY;
    }

    @Override
    public void map(TDataConverter.TResult Serp, Mapper.Context context, boolean hostFlag) throws IOException, InterruptedException {
        int i = 0;
        for (TDataConverter.TClickedLink link : Serp.Clicked) {
            write(context, hostFlag, link.URL, Serp.Query, new Text(String.valueOf(i)));
            ++i;
        }
    }

    @Override
    public void reduce(Text word, Iterable<Text> nums, Reducer.Context context) throws IOException, InterruptedException {
        int sum = 0;
        int cnt = 0;
        for (Text i : nums) {
            sum += Integer.parseInt(i.toString());
            ++cnt;
        }
        context.write(word, new Text(Float.toString((float) sum / cnt)));
    }
}
