import numpy as np
import pandas as pd
import Config
from scipy.sparse import csc_matrix


class TRawDataProcessor:

    def __init__(self):
        self.trainDF = pd.read_csv(Config.TRAIN_MARKS_DATA_PATH,
                                   sep="\t",
                                   names=["QueryID", "DocID", "Relevance"])

        self.testDF = pd.read_csv(Config.TEST_DATA_PATH,
                                  header=1,
                                  sep=",",
                                  names=["QueryID", "DocID"])

        self.trainSamplesCnt = len(self.trainDF.index)
        self.testSamplesCnt = len(self.testDF.index)
        self.samplesCnt = self.trainSamplesCnt + self.testSamplesCnt

        trainSeries = self.trainDF['QueryID'].map(str) + Config.DELIMITER + self.trainDF['DocID'].map(str)
        testSeries = self.testDF['QueryID'].map(str) + Config.DELIMITER + self.testDF['DocID'].map(str)
        series = pd.concat([trainSeries, testSeries])
        self.DataNumeration = {key: idx for idx, key in series.items()}

        self.QuerySetNumeration = TRawDataProcessor.GetPositionsSet(pd.concat([self.trainDF["QueryID"],
                                                                               self.testDF["QueryID"]]))
        self.DocSetNumeration = TRawDataProcessor.GetPositionsSet(pd.concat([self.trainDF["DocID"],
                                                                             self.testDF["DocID"]]))

       self.trainTarget = self.trainDF["Relevance"].to_numpy()

    @staticmethod
    def GetPositionsSet(series: pd.tseries) -> dict:
        SetNumeration = dict()
        for idx, key in series.items():
            if key not in SetNumeration:
                SetNumeration[key] = set()
            SetNumeration[key].add(idx)
        return SetNumeration

    def GetPositions(self, docID: str, queryID: str) -> set:
        positions = self.DocSetNumeration.get(docID, set())
        positions.update(self.QuerySetNumeration.get(queryID, set()))
        positions.update(self.DataNumeration.get(queryID + Config.DELIMITER + docID, set()))
        return positions

    @staticmethod
    def GetFeatureNumber(key: str) -> int:
        position = 0
        if key.startswith(Config.HOST_PREFIX):
            position += len(Config.featureCodes)
            key = key[len(Config.HOST_PREFIX):]
        position += Config.featureCodesPos[key]
        return position

    def process(self) -> [csc_matrix, np.ndarray, csc_matrix]:
        filepath = Config.RAW_DATA_DIRECTORY
        row, col, data = [], [], []

        for path in filepath.glob("*"):
            with path.open("r") as file:
                for line in file:

                    key, value = line[:-1].split("\t")
                    arr = key.split(Config.DELIMITER)
                    # Костыль >>>
                    if len(arr) == 2:
                        arr.append("NONE")
                    # <<< Костыль
                    key_type, queryID, docID = arr

                    featureNum = self.GetFeatureNumber(key_type)
                    positions = self.GetPositions(queryID, docID)
                    for sampleNum in positions:
                        row.append(sampleNum)
                        col.append(featureNum)
                        data.append(value)

        data = csc_matrix((np.array(data), (np.array(row), np.array(col))),
                          shape=(self.samplesCnt, len(Config.featureCodes)))

        return data[:self.trainSamplesCnt], self.trainTarget, data[self.trainSamplesCnt:]
