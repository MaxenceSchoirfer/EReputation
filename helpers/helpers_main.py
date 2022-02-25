from api.twitter_helper import TwitterHelper
from database import local_db_helper as l

# DWH = dwh.DataWarehouseHelper()
# DL = dl.DatalakeHelper()
# DM = dm.DataMartHelper("COCA")

#DWH.create_date_for_year(2022)
#DM.insert_fact_record_twitter(2)

# DL.download_files("COCA")

#L = l.LocalDBHelper()


twitter = TwitterHelper()
twitter.generate_csv("CocaCola","COCA")