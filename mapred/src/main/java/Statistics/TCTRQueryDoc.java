import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

// CTR документа по в выдачах по конкретному запросу
public class TCTRQueryDoc extends BaseStatistic {
    private static final Text ONE_VIEWED = new Text("VIEWED");
    private static final Text ONE_CLICKED = new Text("CLICKED");

    public TCTRQueryDoc(HashMap<String, Integer> URLmap, HashMap<String, Integer> queryMap){
        this.URLMap = URLmap;
        this.QueryMap = queryMap;
        this.KeyPrefix = Config.CTR_QUERY_DOC_KEY;
    }

    public TCTRQueryDoc(){
        this.URLMap = new HashMap<>();
        this.QueryMap = new HashMap<>();
        this.KeyPrefix = Config.CTR_QUERY_DOC_KEY;
    }

    @Override
    public void map(TDataConverter.TResult Serp, Mapper.Context context, boolean hostFlag) throws IOException, InterruptedException {
        for (TDataConverter.TClickedLink item : Serp.Clicked) {
            write(context, hostFlag, item.URL, Serp.Query, ONE_CLICKED);
        }
        for (String item : Serp.Serp) {
            write(context, hostFlag, item, Serp.Query, ONE_VIEWED);
        }
    }

    @Override
    public void reduce(Text word, Iterable<Text> nums, Reducer.Context context) throws IOException, InterruptedException {
        int viewed_cnt = 0;
        int clicked_cnt = 0;
        for (Text value : nums) {
            if (value.equals(ONE_VIEWED)) {
                ++viewed_cnt;
            } else {
                ++clicked_cnt;
            }
        }
        context.write(word, new Text(Float.toString((float) clicked_cnt / viewed_cnt)));
    }
}
