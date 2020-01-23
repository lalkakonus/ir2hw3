import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;


// Среднее число документов, стоящих в выдаче по запросу перед документом, которые были кликнуты после документа
public class TAvgNumClickedAfterQueryDoc extends BaseStatistic {
    public TAvgNumClickedAfterQueryDoc(HashMap<String, Integer> URLmap, HashMap<String, Integer> queryMap){
        this.URLMap = URLmap;
        this.QueryMap = queryMap;
        this.KeyPrefix = Config.AVG_NUMBER_CLICKED_BEFORE_QUERY_DOC_KEY;
    }

    public TAvgNumClickedAfterQueryDoc(){
        this.URLMap = new HashMap<>();
        this.QueryMap = new HashMap<>();
        this.KeyPrefix = Config.AVG_NUMBER_CLICKED_BEFORE_QUERY_DOC_KEY;
    }

    @Override
    public void map(TDataConverter.TResult Serp, Mapper.Context context, boolean hostFlag) throws IOException, InterruptedException {
        Set<Integer> clickedPos = new HashSet<>();
        for (TDataConverter.TClickedLink link: Serp.Clicked) {
            clickedPos.add(link.Position);
        }
        int j = 0;
        for (int i = 0; i < Serp.Serp.size(); ++i) {
            int pos = 0;
            if (clickedPos.contains(i)) {
                ++j;
                pos = Serp.Clicked.size() - j;
            }
            Text value = new Text(String.valueOf(pos));
            write(context, hostFlag, Serp.Serp.get(i), Serp.Query, value);
        }
    }

    @Override
    public void reduce(Text word, Iterable<Text> nums, Reducer.Context context) throws IOException, InterruptedException {
        int sum = 0;
        int cnt = 0;
        for (Text i : nums) {
            sum += Integer.parseInt(i.toString());
            ++cnt;
        }
        context.write(word, new Text(Float.toString((float) sum / cnt)));
    }
}
