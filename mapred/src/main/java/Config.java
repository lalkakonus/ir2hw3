import java.util.Map;
import java.util.HashMap;

public class Config {
    static final String DELIMITER = "-";
    static final public String EMPTY = "EMPTY";
    static final String URL_MAP_FILEPATH = "config_data/url.data/url.data";
    static final String QUERY_MAP_FILEPATH = "config_data/queries.tsv";
    static final String HOST_KEY_PREFIX = "H_";
    // QUERY INDEPENDENT FACTORS
    static final String IMPLEMENTATION_DOC_KEY = "A1";
    static final String CLICK_DOC_KEY = "A2";
    static final String CTR_DOC_KEY = "A3";
    static final String AVG_VIEW_TIME_DOC_KEY = "A4";
    static final String AVG_POS_DIFF_LAST_DOC = "A5";
    static final String AVG_POS_DIFF_NEXT_DOC = "A6";
    // QUERY DEPENDENT FACTORS
    static final String CTR_QUERY_DOC_KEY = "B1";
    static final String FIRST_CTR_QUERY_DOC_KEY = "B2";
    static final String LAST_CTR_QUERY_DOC_KEY = "B3";
    static final String ONLY_CTR_QUERY_DOC_KEY = "B4";
    static final String PERCENT_CLICK_QUERY_DOC_KEY = "B5";
    // TIME BASED FACTORS
    static final String AVG_VIEW_TIME_QUERY_DOC_KEY = "C1";
    // POSITION AND CLICK NUMBER BASED FACTOR
    static final String AVG_POSITION_QUERY_DOC_KEY = "D1";
    static final String AVG_CLICK_POSITION_QUERY_DOC_KEY = "D2";
    static final String AVG_INVERSE_CLICK_POSITION_QUERY_DOC_KEY = "D3";
    static final String AVG_NUMBER_CLICKED_BEFORE_QUERY_DOC_KEY = "D4";
    static final String AVG_NUMBER_CLICKED_AFTER_QUERY_DOC_KEY = "D5";
    // PROBABILISTIC BASED FACTORS
    static final String PROB_LAST_CLICKED_QUERY_DOC_KEY = "E1";
    static final String PROB_UP_CLICK_QUERY_DOC_KEY = "E2";
    static final String PROB_DOWN_QUERY_DOC_KEY = "E3";
    static final String PROB_DOUBLE_CLICK_QUERY_DOC_KEY = "E4";
    static final String PROB_COME_BACK_QUERY_DOC_KEY = "E5";
    static final String PROB_GO_BACK_QUERY_DOC_KEY = "E6";
    // QUERY AND SERP DEPEND FACTORS
    static final String AVG_CLICK_POSITION_QUERY_KEY = "F1";
    static final String AVG_WORK_TIME_QUERY_KEY = "F2";
    static final String AVG_FIRST_POSITION_QUERY_KEY = "F3";
    static final String AVG_CLICK_COUNT_QUERY_KEY = "F4";


    static Map<String, Class> ClassMap;
    static {
        ClassMap = new HashMap<>();
        // QUERY INDEPENDENT FACTORS
        ClassMap.put(IMPLEMENTATION_DOC_KEY, TImplementationDoc.class);
        ClassMap.put(CLICK_DOC_KEY, TClickDoc.class);
        ClassMap.put(CTR_DOC_KEY, TCTRDoc.class);
        ClassMap.put(AVG_VIEW_TIME_DOC_KEY, TAvgViewTimeDoc.class);
        ClassMap.put(AVG_POS_DIFF_LAST_DOC, TAvgPosDiffLastDoc.class);
        ClassMap.put(AVG_POS_DIFF_NEXT_DOC, TAvgPosDiffNextDoc.class);
        // QUERY DEPENDENT FACTORS
        ClassMap.put(CTR_QUERY_DOC_KEY, TCTRQueryDoc.class);
        ClassMap.put(FIRST_CTR_QUERY_DOC_KEY, TFirstCTRQueryDoc.class);
        ClassMap.put(LAST_CTR_QUERY_DOC_KEY, TLastCTRQueryDoc.class);
        ClassMap.put(ONLY_CTR_QUERY_DOC_KEY, TOnlyCTRQueryDoc.class);
        ClassMap.put(PERCENT_CLICK_QUERY_DOC_KEY, TPercentClickQueryDoc.class);
        // TIME BASED FACTORS
        ClassMap.put(AVG_VIEW_TIME_QUERY_DOC_KEY, TAvgViewTimeQueryDoc.class);
        // POSITION AND CLICK NUMBER BASED FACT
        ClassMap.put(AVG_POSITION_QUERY_DOC_KEY, TAvgPositionQueryDoc.class);
        ClassMap.put(AVG_CLICK_POSITION_QUERY_DOC_KEY, TAvgClickPositionQueryDoc.class);
        ClassMap.put(AVG_INVERSE_CLICK_POSITION_QUERY_DOC_KEY, TAvgInverseClickPositionQueryDoc.class);
        ClassMap.put(AVG_NUMBER_CLICKED_BEFORE_QUERY_DOC_KEY, TAvgNumClickedBeforeQueryDoc.class);
        ClassMap.put(AVG_NUMBER_CLICKED_AFTER_QUERY_DOC_KEY, TAvgNumClickedAfterQueryDoc.class);
        // PROBABILISTIC BASED FACTORS
        ClassMap.put(PROB_LAST_CLICKED_QUERY_DOC_KEY, TProbLastClickedQueryDoc.class);
        ClassMap.put(PROB_UP_CLICK_QUERY_DOC_KEY, TProbUpClickQueryDoc.class);
        ClassMap.put(PROB_DOWN_QUERY_DOC_KEY, TProbDownQueryDoc.class);
        ClassMap.put(PROB_DOUBLE_CLICK_QUERY_DOC_KEY, TProbDoubleClickQueryDoc.class);
        ClassMap.put(PROB_COME_BACK_QUERY_DOC_KEY, TProbComeBackQueryDoc.class);
        ClassMap.put(PROB_GO_BACK_QUERY_DOC_KEY, TProbGoBackQueryDoc.class);
        // QUERY AND SERP DEPEND FACTORS
        ClassMap.put(AVG_CLICK_POSITION_QUERY_KEY, TAvgClickPositionQuery.class);
        ClassMap.put(AVG_WORK_TIME_QUERY_KEY, TAvgWorkTimeQuery.class);
        ClassMap.put(AVG_FIRST_POSITION_QUERY_KEY, TAvgFirstPositionQuery.class);
        ClassMap.put(AVG_CLICK_COUNT_QUERY_KEY, TAvgClickCountQuery.class);
    }
}
