import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;


// Число кликов на документ по всем выдачам, в которых он встречался.
public class TClickDoc extends BaseStatistic {
    private static final Text one = new Text("1");
    public TClickDoc(HashMap<String, Integer> URLmap, HashMap<String, Integer> queryMap){
        this.URLMap = URLmap;
        this.QueryMap = queryMap;
        this.KeyPrefix = Config.CLICK_DOC_KEY;
    }

    public TClickDoc(){
        this.URLMap = new HashMap<>();
        this.QueryMap = new HashMap<>();
        this.KeyPrefix = Config.CLICK_DOC_KEY;
    }

    @Override
    public void map(TDataConverter.TResult Serp, Mapper.Context context, boolean hostFlag) throws IOException, InterruptedException {
        for (TDataConverter.TClickedLink item : Serp.Clicked) {
            write(context, hostFlag, item.URL, Config.EMPTY, one);
        }
    }

    @Override
    public void reduce(Text word, Iterable<Text> nums, Reducer.Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (Text i : nums) {
            ++sum;
        }
        context.write(word, new Text(Integer.toString(sum)));
    }
}
