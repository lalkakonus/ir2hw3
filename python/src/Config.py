from pathlib import Path

DELIMITER = "-"
HOST_PREFIX = "H_"
ROOT = Path("/home/sergey/mail-sphere/ir3/hw3")
RAW_DATA_DIRECTORY = ROOT / "data/out"
DATA_DIRECTORY = ROOT / "data"
TRAIN_MARKS_DATA_PATH = DATA_DIRECTORY / "train.marks.tsv/train.marks.tsv"
TEST_DATA_PATH = DATA_DIRECTORY / "sample.csv/sample.csv"

featureCodes = {
    "A1": "IMPLEMENTATION_DOC",
    "A2": "CLICK_DOC",
    "A3": "CTR_DOC",
    "A4": "AVG_VIEW_TIME_DOC",
    "A5": "AVG_POS_DIFF_LAST_DOC",
    "A6": "AVG_POS_DIFF_NEXT_DOC",
    # QUERY
    "B1": "CTR_QUERY_DOC",
    "B2": "FIRST_CTR_QUERY_DOC",
    "B3": "LAST_CTR_QUERY_DOC",
    "B4": "ONLY_CTR_QUERY_DOC",
    "B5": "PERCENT_CLICK_QUERY_DOC",
    # TIME BASED
    "C1": "AVG_VIEW_TIME_QUERY_DOC",
    # POSITION AND CLICK NUMBER BASED FACTOR
    "D1": "AVG_POSITION_QUERY_DOC",
    "D2": "AVG_CLICK_POSITION_QUERY_DOC",
    "D3": "AVG_INVERSE_CLICK_POSITION_QUERY_DOC",
    "D4": "AVG_NUMBER_CLICKED_BEFORE_QUERY_DOC",
    "D5": "AVG_NUMBER_CLICKED_AFTER_QUERY_DOC",
    # PROBABILISTIC BASED FACTORS
    "E1": "PROB_LAST_CLICKED_QUERY_DOC_KEY",
    "E2": "PROB_UP_CLICK_QUERY_DOC",
    "E3": "PROB_DOWN_QUERY_DOC",
    "E4": "PROB_DOUBLE_CLICK_QUERY_DOC",
    "E5": "PROB_COME_BACK_QUERY_DOC",
    "E6": "PROB_GO_BACK_QUERY_DOC",
    # QUERY AND SERP DEPEND FACTOR
    "F1": "AVG_CLICK_POSITION_QUERY",
    "F2": "AVG_WORK_TIME_QUERY",
    "F3": "AVG_FIRST_POSITION_QUERY",
    "F4": "AVG_CLICK_COUNT_QUERY"
}
featureCodesPos = {
    "A1": 0,
    "A2": 1,
    "A3": 2,
    "A4": 3,
    "A5": 4,
    "A6": 5,
    # QUERY
    "B1": 6,
    "B2": 7,
    "B3": 8,
    "B4": 9,
    "B5": 10,
    # TIME BASED
    "C1": 11,
    # POSITION AND CLICK NUMBER BASED FACTOR
    "D1": 12,
    "D2": 13,
    "D3": 14,
    "D4": 15,
    "D5": 16,
    # PROBABILISTIC BASED FACTORS
    "E1": 17,
    "E2": 18,
    "E3": 19,
    "E4": 20,
    "E5": 21,
    "E6": 22,
    # QUERY AND SERP DEPEND FACTOR
    "F1": 23,
    "F2": 24,
    "F3": 25,
    "F4": 26
}

assert (DATA_DIRECTORY.exists(), "Data directory ({}) absent".format(DATA_DIRECTORY))
assert (RAW_DATA_DIRECTORY.exists(), "Raw data directory ({}) absent".format(RAW_DATA_DIRECTORY))
assert (TRAIN_MARKS_DATA_PATH.exists(), "Train marks data ({}) absent".format(TRAIN_MARKS_DATA_PATH))
assert (TEST_DATA_PATH.exists(), "Test marks data ({}) absent".format(TEST_DATA_PATH))