import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;


// Доля кликов по документу в выдачах по текущему запросу
public class TPercentClickQueryDoc extends BaseStatistic {
    private static final String ALL_CLICKED = "ALL_";
    private static final Text ONE_CLICKED = new Text("ONE");

    public TPercentClickQueryDoc(HashMap<String, Integer> URLmap, HashMap<String, Integer> queryMap){
        this.URLMap = URLmap;
        this.QueryMap = queryMap;
        this.KeyPrefix = Config.PERCENT_CLICK_QUERY_DOC_KEY;
    }

    public TPercentClickQueryDoc(){
        this.URLMap = new HashMap<>();
        this.QueryMap = new HashMap<>();
        this.KeyPrefix = Config.PERCENT_CLICK_QUERY_DOC_KEY;
    }

    @Override
    public void map(TDataConverter.TResult Serp, Mapper.Context context, boolean hostFlag) throws IOException, InterruptedException {
        for (TDataConverter.TClickedLink item : Serp.Clicked) {
            write(context, hostFlag, item.URL, Serp.Query, ONE_CLICKED);
        }
        for (String item : Serp.Serp) {
            Text value = new Text(ALL_CLICKED + Serp.Clicked.size());
            write(context, hostFlag, item, Serp.Query, value);
        }
    }

    @Override
    public void reduce(Text word, Iterable<Text> nums, Reducer.Context context) throws IOException, InterruptedException {
        int all_clicked_cnt = 0;
        int clicked_cnt = 0;
        for (Text value : nums) {
            if (value.equals(ONE_CLICKED)) {
                ++clicked_cnt;
            } else {
                all_clicked_cnt += Integer.parseInt(value.toString().substring(ALL_CLICKED.length()));
            }
        }
        context.write(word, new Text(Float.toString((float) clicked_cnt / all_clicked_cnt)));
    }
}
