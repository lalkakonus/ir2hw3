import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

// CTR, когда документ кликается единственным в выдаче по конкретному запросу
public class TOnlyCTRQueryDoc extends BaseStatistic {
    private static final Text ONE_VIEWED = new Text("VIEWED");
    private static final Text ONE_CLICKED = new Text("CLICKED");

    public TOnlyCTRQueryDoc(HashMap<String, Integer> URLmap, HashMap<String, Integer> queryMap){
        this.URLMap = URLmap;
        this.QueryMap = queryMap;
        this.KeyPrefix = Config.ONLY_CTR_QUERY_DOC_KEY;
    }

    public TOnlyCTRQueryDoc(){
        this.URLMap = new HashMap<>();
        this.QueryMap = new HashMap<>();
        this.KeyPrefix = Config.ONLY_CTR_QUERY_DOC_KEY;
    }

    @Override
    public void map(TDataConverter.TResult Serp, Mapper.Context context, boolean hostFlag) throws IOException, InterruptedException {
        if (Serp.Clicked.size() == 1) {
            write(context, hostFlag, Serp.Clicked.get(0).URL, Serp.Query, ONE_CLICKED);
        }
        for (String item : Serp.Serp) {
            write(context, hostFlag, item, Serp.Query, ONE_VIEWED);
        }
    }

    @Override
    public void reduce(Text word, Iterable<Text> nums, Reducer.Context context) throws IOException, InterruptedException {
        int viewed_cnt = 0;
        int clicked_cnt = 0;
        for (Text i : nums) {
            if (i.toString().equals(ONE_VIEWED)) {
                ++viewed_cnt;
            } else {
                ++clicked_cnt;
            }
        }

        context.write(word, new Text(Float.toString((float) clicked_cnt / viewed_cnt)));
    }
}
