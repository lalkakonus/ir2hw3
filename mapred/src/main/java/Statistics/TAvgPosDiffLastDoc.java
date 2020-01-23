import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;


// Среднее число позиций до предыдущего кликнутого документа
public class TAvgPosDiffLastDoc extends BaseStatistic {
    public TAvgPosDiffLastDoc(HashMap<String, Integer> URLmap, HashMap<String, Integer> queryMap){
        this.URLMap = URLmap;
        this.QueryMap = queryMap;
        this.KeyPrefix = Config.AVG_POS_DIFF_LAST_DOC;
    }

    public TAvgPosDiffLastDoc(){
        this.URLMap = new HashMap<>();
        this.QueryMap = new HashMap<>();
        this.KeyPrefix = Config.AVG_POS_DIFF_LAST_DOC;
    }

    @Override
    public void map(TDataConverter.TResult Serp, Mapper.Context context, boolean hostFlag) throws IOException, InterruptedException {
        if (!Serp.Clicked.isEmpty()) {
            int last = Serp.Clicked.get(0).Position;
            for (TDataConverter.TClickedLink link : Serp.Clicked) {
                Text value = new Text(Integer.toString(Math.abs(last - link.Position)));
                write(context, hostFlag, link.URL, Config.EMPTY, value);
                last = link.Position;
            }
        }
    }

    @Override
    public void reduce(Text word, Iterable<Text> nums, Reducer.Context context) throws IOException, InterruptedException {
        long sum_diff = 0;
        int clicked_cnt = 0;
        for (Text value : nums) {
            sum_diff += Math.log(1 + Long.parseLong(value.toString()));
            ++clicked_cnt;
        }
        context.write(word, new Text(Long.toString(sum_diff / clicked_cnt)));
    }
}
