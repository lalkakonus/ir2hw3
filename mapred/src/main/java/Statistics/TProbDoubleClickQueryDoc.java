import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;


// Вероятность того, что по документу кликнули два раза подряд
public class TProbDoubleClickQueryDoc extends BaseStatistic {
    private static final Text CLICKED = new Text("CLICKED");
    private static final Text DOUBLE_CLICKED = new Text("DOUBLE_CLICKED");

    public TProbDoubleClickQueryDoc(HashMap<String, Integer> URLmap, HashMap<String, Integer> queryMap){
        this.URLMap = URLmap;
        this.QueryMap = queryMap;
        this.KeyPrefix = Config.PROB_DOUBLE_CLICK_QUERY_DOC_KEY;
    }

    public TProbDoubleClickQueryDoc(){
        this.URLMap = new HashMap<>();
        this.QueryMap = new HashMap<>();
        this.KeyPrefix = Config.PROB_DOUBLE_CLICK_QUERY_DOC_KEY;
    }

    @Override
    public void map(TDataConverter.TResult Serp, Mapper.Context context, boolean hostFlag) throws IOException, InterruptedException {
        if (!Serp.Clicked.isEmpty()) {
            for (int i = 0; i < Serp.Clicked.size() - 1; ++i) {
                write(context, hostFlag, Serp.Clicked.get(i).URL, Serp.Query, CLICKED);
                if (Serp.Clicked.get(i).URL.equals(Serp.Clicked.get(i + 1).URL)) {
                    write(context, hostFlag, Serp.Clicked.get(i).URL, Serp.Query, DOUBLE_CLICKED);
                }
            }
            write(context, hostFlag, Serp.Clicked.get(Serp.Clicked.size() - 1).URL, Serp.Query, CLICKED);
        }
    }

    @Override
    public void reduce(Text word, Iterable<Text> nums, Reducer.Context context) throws IOException, InterruptedException {
        int double_clicked_cnt = 0;
        int clicked_cnt = 0;
        for (Text i : nums) {
            if (i.equals(CLICKED)) {
                ++clicked_cnt;
            } else {
                ++double_clicked_cnt;
            }
        }
        context.write(word, new Text(Float.toString((float) double_clicked_cnt / clicked_cnt)));
    }
}
