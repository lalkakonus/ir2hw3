import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

// Вероятность клика на документ, находящиеся в выдаче по текущему запросу на позицию ниже
public class TProbDownQueryDoc extends BaseStatistic {
    private static final Text VIEWED = new Text("VIEWED");
    private static final Text CLICKED_LOWER = new Text("CLICKED_LOWER");

    public TProbDownQueryDoc(HashMap<String, Integer> URLmap, HashMap<String, Integer> queryMap){
        this.URLMap = URLmap;
        this.QueryMap = queryMap;
        this.KeyPrefix = Config.PROB_DOWN_QUERY_DOC_KEY;
    }

    public TProbDownQueryDoc(){
        this.URLMap = new HashMap<>();
        this.QueryMap = new HashMap<>();
        this.KeyPrefix = Config.PROB_DOWN_QUERY_DOC_KEY;
    }

    @Override
    public void map(TDataConverter.TResult Serp, Mapper.Context context, boolean hostFlag) throws IOException, InterruptedException {
        for (String link : Serp.Serp) {
            write(context, hostFlag, link, Serp.Query, VIEWED);
        }
        for (TDataConverter.TClickedLink link : Serp.Clicked) {
            int idx = Serp.Serp.indexOf(link.URL);
            if (idx > 0) {
                write(context, hostFlag, Serp.Serp.get(idx - 1), Serp.Query, CLICKED_LOWER);
            }
        }
    }

    @Override
    public void reduce(Text word, Iterable<Text> nums, Reducer.Context context) throws IOException, InterruptedException {
        int clicked_lower = 0;
        int viewed_cnt = 0;
        for (Text i : nums) {
            if (i.equals(VIEWED)) {
                ++viewed_cnt;
            } else {
                ++clicked_lower;
            }
        }
        context.write(word, new Text(Float.toString((float) clicked_lower / viewed_cnt)));
    }
}
