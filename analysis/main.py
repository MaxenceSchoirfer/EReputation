import sentiment_analysis
from database.database_helper import DatabaseHelper

db = DatabaseHelper()
sentiment_analysis.analysis("../data/twitter/TWITTER_COCA_18-01-2022_UNK_EN.csv")
