import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;


public class BaseStatistic {

    protected HashMap<String, Integer> URLMap;
    protected HashMap<String, Integer> QueryMap;
    protected String KeyPrefix;

    protected void write(Mapper.Context context,
                         boolean hostFlag,
                         String url,
                         String query,
                         Text value) throws IOException, InterruptedException {
        boolean queryPredicate = query.equals(Config.EMPTY) || QueryMap.containsKey(query);
        boolean urlPredicate = url.equals(Config.EMPTY) || URLMap.containsKey(url);
        if (!(urlPredicate && queryPredicate)) {
            return;
        }

        String key = "";
        if (hostFlag) key += Config.HOST_KEY_PREFIX;
        key += KeyPrefix;
        key += Config.DELIMITER + (query.equals(Config.EMPTY) ? Config.EMPTY : QueryMap.get(query));
        key += Config.DELIMITER + (url.equals(Config.EMPTY) ? Config.EMPTY : URLMap.get(url));
        context.write(new Text(key), value);
    }

    public void map(TDataConverter.TResult Serp,
                    Mapper.Context context,
                    boolean host) throws IOException, InterruptedException {
    }

    public void reduce(Text word,
                       Iterable<Text> nums,
                       Reducer.Context context) throws IOException, InterruptedException {
    }
}
