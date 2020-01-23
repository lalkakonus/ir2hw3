import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


// Число показов документа по всем выдачам, в которых он встречался
public class TImplementationDoc extends BaseStatistic {
    private static final Text ONE = new Text("1");

    public TImplementationDoc(){
        this.URLMap = new HashMap<>();
        this.QueryMap = new HashMap<>();
        this.KeyPrefix = Config.IMPLEMENTATION_DOC_KEY;
    }

    public TImplementationDoc(HashMap<String, Integer> URLmap, HashMap<String, Integer> queryMap){
        this.URLMap = URLmap;
        this.QueryMap = queryMap;
        this.KeyPrefix = Config.IMPLEMENTATION_DOC_KEY;
    }

    @Override
    public void map(TDataConverter.TResult Serp, Mapper.Context context, boolean hostFlag) throws IOException, InterruptedException {
        for (String url : Serp.Serp) {
            write(context, hostFlag, url, Config.EMPTY, ONE);
        }
    }

    @Override
    public void reduce(Text word, Iterable<Text> nums, Reducer.Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (Text i : nums) {
            ++sum;
        }
        context.write(word, new Text(Integer.toString(sum)));
    }
}