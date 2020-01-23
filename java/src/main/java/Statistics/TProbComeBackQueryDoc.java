import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;


// Вероятность того, что к документу вернулись после клика по одной из нижерасположенных ссылок
public class TProbComeBackQueryDoc extends BaseStatistic {
    private static final Text CLICKED = new Text("CLICKED");
    private static final Text BACK = new Text("BACK");

    public TProbComeBackQueryDoc(HashMap<String, Integer> URLmap, HashMap<String, Integer> queryMap){
        this.URLMap = URLmap;
        this.QueryMap = queryMap;
        this.KeyPrefix = Config.PROB_COME_BACK_QUERY_DOC_KEY;
    }

    public TProbComeBackQueryDoc(){
        this.URLMap = new HashMap<>();
        this.QueryMap = new HashMap<>();
        this.KeyPrefix = Config.PROB_COME_BACK_QUERY_DOC_KEY;
    }

    @Override
    public void map(TDataConverter.TResult Serp, Mapper.Context context, boolean hostFlag) throws IOException, InterruptedException {
        int maxClickedPos = -1;
        for (TDataConverter.TClickedLink link : Serp.Clicked) {
            write(context, hostFlag, link.URL, Serp.Query, CLICKED);
            if (link.Position < maxClickedPos) {
                write(context, hostFlag, link.URL, Serp.Query, BACK);
            }
            maxClickedPos = Math.max(maxClickedPos, link.Position);
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
