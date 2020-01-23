import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;


// Средний номер клика в документ с конца в выдачаи по текущему запросу
public class TAvgInverseClickPositionQueryDoc extends BaseStatistic {
    public TAvgInverseClickPositionQueryDoc(HashMap<String, Integer> URLmap, HashMap<String, Integer> queryMap){
        this.URLMap = URLmap;
        this.QueryMap = queryMap;
        this.KeyPrefix = Config.AVG_INVERSE_CLICK_POSITION_QUERY_DOC_KEY;
    }

    public TAvgInverseClickPositionQueryDoc(){
        this.URLMap = new HashMap<>();
        this.QueryMap = new HashMap<>();
        this.KeyPrefix = Config.AVG_INVERSE_CLICK_POSITION_QUERY_DOC_KEY;
    }

    @Override
    public void map(TDataConverter.TResult Serp, Mapper.Context context, boolean hostFlag) throws IOException, InterruptedException {
        int i = 0;
        for (TDataConverter.TClickedLink link : Serp.Clicked) {
            Text value = new Text(String.valueOf(Serp.Serp.size() - (i + 1)));
            write(context, hostFlag, link.URL, Serp.Query, value);
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
