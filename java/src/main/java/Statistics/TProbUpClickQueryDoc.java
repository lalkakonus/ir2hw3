import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

// Вероятность клика на документ, находящийся в выдаче по запросу на позицию выше текущего
public class TProbUpClickQueryDoc extends BaseStatistic {
    private static final Text VIEWED = new Text("VIEWED");
    private static final Text CLICKED_UPPER = new Text("CLICKED_UPPER");

    public TProbUpClickQueryDoc(HashMap<String, Integer> URLmap, HashMap<String, Integer> queryMap){
        this.URLMap = URLmap;
        this.QueryMap = queryMap;
        this.KeyPrefix = Config.PROB_UP_CLICK_QUERY_DOC_KEY;
    }

    public TProbUpClickQueryDoc(){
        this.URLMap = new HashMap<>();
        this.QueryMap = new HashMap<>();
        this.KeyPrefix = Config.PROB_UP_CLICK_QUERY_DOC_KEY;
    }

    @Override
    public void map(TDataConverter.TResult Serp, Mapper.Context context, boolean hostFlag) throws IOException, InterruptedException {
        for (String link : Serp.Serp) {
            write(context, hostFlag, link, Serp.Query, VIEWED);
        }
        int size = Serp.Serp.size();
        for (TDataConverter.TClickedLink link : Serp.Clicked) {
            int idx = Serp.Serp.indexOf(link.URL);
            if (0 <= idx && idx + 1 < size) {
                write(context, hostFlag, Serp.Serp.get(idx + 1), Serp.Query, CLICKED_UPPER);
            }
        }
    }

    @Override
    public void reduce(Text word, Iterable<Text> nums, Reducer.Context context) throws IOException, InterruptedException {
        int clicked_upper = 0;
        int viewed_cnt = 0;
        for (Text i : nums) {
            if (i.equals(VIEWED)) {
                ++viewed_cnt;
            } else {
                ++clicked_upper;
            }
        }
        context.write(word, new Text(Float.toString((float) clicked_upper / viewed_cnt)));
    }
}
