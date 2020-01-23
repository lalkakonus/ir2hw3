from RawDataProcesser import TRawDataProcessor
import Config


def main():
    dataProcessor = TRawDataProcessor()
    X_train, y_train, X_test = dataProcessor.process()
    pass


if __name__ == "__main__":
    main()
