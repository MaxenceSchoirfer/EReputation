import sentiment_analysis
from database.data_warehouse_helper import DataWarehouseHelper

db = DataWarehouseHelper()
sentiment_analysis.analysis("../data/twitter/TWITTER_COCA_18-01-2022_UNK_EN.csv")
