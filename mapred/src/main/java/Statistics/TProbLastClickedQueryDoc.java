import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;


// Вероятность быть последним документом, кликнутым по текущему запросу
public class TProbLastClickedQueryDoc extends BaseStatistic {
    private static final Text CLICKED = new Text("CLICKED");
    private static final Text CLICKED_LAST = new Text("CLICKED_LAST");

    public TProbLastClickedQueryDoc(HashMap<String, Integer> URLmap, HashMap<String, Integer> queryMap){
        this.URLMap = URLmap;
        this.QueryMap = queryMap;
        this.KeyPrefix = Config.PROB_LAST_CLICKED_QUERY_DOC_KEY;
    }

    public TProbLastClickedQueryDoc(){
        this.URLMap = new HashMap<>();
        this.QueryMap = new HashMap<>();
        this.KeyPrefix = Config.PROB_LAST_CLICKED_QUERY_DOC_KEY;
    }

    @Override
    public void map(TDataConverter.TResult Serp, Mapper.Context context, boolean hostFlag) throws IOException, InterruptedException {
        if (!Serp.Clicked.isEmpty()) {
            int size = Serp.Clicked.size();
            for (int i = 0; i < size; ++i) {
                write(context, hostFlag, Serp.Clicked.get(i).URL, Serp.Query, CLICKED);
            }
            write(context, hostFlag, Serp.Clicked.get(size - 1).URL, Serp.Query, CLICKED_LAST);
        }
    }


    @Override
    public void reduce(Text word, Iterable<Text> nums, Reducer.Context context) throws IOException, InterruptedException {
        int last_clicked_cnt = 0;
        int clicked_cnt = 0;
        for (Text i : nums) {
            if (i.equals(CLICKED)) {
                ++clicked_cnt;
            } else {
                ++last_clicked_cnt;
            }
        }
        context.write(word, new Text(Float.toString((float) clicked_cnt / last_clicked_cnt)));
    }
}
