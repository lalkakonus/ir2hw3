import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.ListIterator;


// Вероятность того, что после клика на документ, пользователь кликал по документам расположенным выше него
public class TProbGoBackQueryDoc extends BaseStatistic {
    private static final Text CLICKED = new Text("CLICKED");
    private static final Text OTHER_CLICKED = new Text("OTHER_CLICKED");

    public TProbGoBackQueryDoc(HashMap<String, Integer> URLmap, HashMap<String, Integer> queryMap){
        this.URLMap = URLmap;
        this.QueryMap = queryMap;
        this.KeyPrefix = Config.PROB_GO_BACK_QUERY_DOC_KEY;
    }

    public TProbGoBackQueryDoc(){
        this.URLMap = new HashMap<>();
        this.QueryMap = new HashMap<>();
        this.KeyPrefix = Config.PROB_GO_BACK_QUERY_DOC_KEY;
    }

    @Override
    public void map(TDataConverter.TResult Serp, Mapper.Context context, boolean hostFlag) throws IOException, InterruptedException {
        ListIterator<TDataConverter.TClickedLink> it = Serp.Clicked.listIterator(Serp.Clicked.size());
        int min_pos = Serp.Serp.size() + 1;
        while (it.hasPrevious()) {
            TDataConverter.TClickedLink link = it.previous();
            write(context, hostFlag, link.URL, Serp.Query, CLICKED);
            if (link.Position > min_pos) {
                write(context, hostFlag, link.URL, Serp.Query, OTHER_CLICKED);
            }
            min_pos = Math.min(min_pos, link.Position);
        }
    }

    @Override
    public void reduce(Text word, Iterable<Text> nums, Reducer.Context context) throws IOException, InterruptedException {
        int other_clicked_cnt = 0;
        int clicked_cnt = 0;
        for (Text i : nums) {
            if (i.equals(CLICKED)) {
                ++clicked_cnt;
            } else {
                ++other_clicked_cnt;
            }
        }
        context.write(word, new Text(Float.toString((float) other_clicked_cnt / clicked_cnt)));
    }
}