import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.ListIterator;


// Среднее число позиций до следующего кликнутого документа
public class TAvgPosDiffNextDoc extends BaseStatistic {
    public TAvgPosDiffNextDoc(HashMap<String, Integer> URLmap, HashMap<String, Integer> queryMap){
        this.URLMap = URLmap;
        this.QueryMap = queryMap;
        this.KeyPrefix = Config.AVG_POS_DIFF_NEXT_DOC;
    }

    public TAvgPosDiffNextDoc(){
        this.URLMap = new HashMap<>();
        this.QueryMap = new HashMap<>();
        this.KeyPrefix = Config.AVG_POS_DIFF_NEXT_DOC;
    }

    @Override
    public void map(TDataConverter.TResult Serp, Mapper.Context context, boolean hostFlag) throws IOException, InterruptedException {
        if (!Serp.Clicked.isEmpty()) {
            TDataConverter.TClickedLink current = Serp.Clicked.get(Serp.Clicked.size() - 1);
            int last = current.Position;
            ListIterator<TDataConverter.TClickedLink> it = Serp.Clicked.listIterator(Serp.Clicked.size() - 1);
            while (it.hasPrevious()) {
                Text value = new Text(Integer.toString(Math.abs(last - current.Position)));
                write(context, hostFlag, current.URL, Config.EMPTY, value);
                last = current.Position;
                current = it.previous();
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