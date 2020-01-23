import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TDataConverter {

    public static HashMap<String, Integer> CreateMap(Path filepath) {
        HashMap<String, Integer> map = new HashMap<>();

        try {
            FileSystem fs = FileSystem.get(new Configuration());
            FSDataInputStream inputStream = fs.open(filepath);
            String out = IOUtils.toString(inputStream, "UTF-8");
//            File file = new File(filepath);
//            FileReader fileReader = new FileReader(file);
//            BufferedReader bufferedReader = new BufferedReader(fileReader);
//
//            String line;
//            while ((line = bufferedReader.readLine()) != null) {
            for (String line : out.split("\n")) {
                String[] parts = line.split("\\t");
                map.put(parts[1], Integer.parseInt(parts[0]));
            }
            inputStream.close();
//            bufferedReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return map;
    }

    public static class TResult {
        public String Query;
        public Integer Region;
        public List<String> Serp;
        public List<TClickedLink> Clicked;

        TResult() {
            Serp = new ArrayList<>();
            Clicked = new ArrayList<>();
        }
    }

    public static class TClickedLink {
        public String URL;
        public Long Timestamp;
        public Integer Position;

        TClickedLink(String url, Long timestamp, Integer position) {
            URL = url;
            Timestamp = timestamp;
            Position = position;
        }
    }

    private static String ExtractHost(String url) {
        int pos = url.indexOf("/");
        if (pos >= 0) {
            url = url.substring(0, pos);
        }
        return url;
    }

    private static String CutURL(String URL) {
        return URL.replaceAll("(^https?\\:\\/\\/|\\/$)", "");
    }

    static TResult Convert(String input) throws IOException {
        TResult result = new TResult();

        String[] resolve = input.split("@");
        result.Query = resolve[0];
        resolve = resolve[1].split("\\t");
        result.Region = Integer.parseInt(resolve[0]);
        for (String url : resolve[1].split(",")) {
            url = CutURL(url);
            result.Serp.add(url);
        }

        if (resolve.length > 2) {
            for (String url : resolve[2].split(",")) {
                url = CutURL(url);
                result.Clicked.add(new TClickedLink(url, 0L, 0));
            }

            int i = 0;
            for (String timestamp : resolve[3].split(",")) {
                result.Clicked.get(i).Timestamp = Long.parseLong(timestamp);
                ++i;
            }

            for (TClickedLink link : result.Clicked) {
                link.Position = result.Serp.indexOf(link.URL);
            }

            result.Clicked.sort((link1, link2) -> link1.Timestamp.compareTo(link2.Timestamp));
        }

        return result;
    }

    static TResult ConvertToHost(String input) throws IOException {
        TResult serp = Convert(input);
        for (TClickedLink link : serp.Clicked) {
            link.URL = ExtractHost(link.URL);
        }
        for (int i = 0; i < serp.Serp.size(); ++i) {
            serp.Serp.set(i, ExtractHost(serp.Serp.get(i)));
        }
        return serp;
    }

}