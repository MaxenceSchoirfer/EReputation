import logging
import sys
from datetime import date, datetime

from analysis import analysis_helper
from api.twitter_helper import TwitterHelper
from data.dataset import Dataset
from database.local_db_helper import LocalDBHelper
from helpers.datalake_helper import DatalakeHelper
from helpers.datawarehouse_helper import DataWarehouseHelper


def log(message, level):
    if True:
        print(message)
    if level == "INFO":
        logging.info(message)
    elif level == "ERROR":
        logging.error(message)


def processing(alias, keyword):
    # ---------------------------------------- CALL API ---------------------------------------

    try:
        log("Start fetching data from API [source : TWITTER, client : " + alias + ", date : " + date + "]", "INFO")
        twitter_helper.generate_csv(keyword, alias)
    except Exception as e:
        log("Error occurred during fetching data from API [source : TWITTER, client : " + alias + ", date : " + date + "] -> " + str(
            e), "ERROR")
        return

        # ----------------------------------------  DOWNLOAD FILES LOCALLY -------------------------------------------------------------
    try:
        log("Start uploading files on the datalake [source : TWITTER, client : " + alias + ", date : " + date + "]",
            "INFO")
        files = datalake_helper.get_local_filenames(alias, date)

        # todo try to correct error during uploading
        # for file in files:
        #     datalake_helper.upload_file(file)

    except Exception as e:
        log("Error occurred during fetching files from datalake [source : TWITTER, client : " + alias + ", date : " + date + "] -> " + str(
            e), "ERROR")
        return

    # ------------------------------------- ANALYSIS ------------------------------------------------------------------

    for file in files:
        try:
            log("Start analysis [file : " + file + ", source : TWITTER, client : " + alias + ", date : " + date + "]",
                "INFO")
            dataset = Dataset(file, False)
            analysis_helper.analysis(dataset)
        except Exception as e:
            log("Error occurred during analysis [file : " + file + ", source : TWITTER, client : " + alias + ", date : " + date + "] -> " + str(
                e), "ERROR")
            continue

        try:
            log("Start storage of analyse results [file : " + dataset.file + ", source : TWITTER, client : " + dataset.client + ", date : " + date + "]",
                "INFO")
            dwh_helper.save_dataset_analysis(dataset)
        except Exception as e:
            log("Error occurred during storage of analyse results  [file : " + dataset.file + ", source : TWITTER, client : " + dataset.client + ", date : " + date + "] -> " + str(
                e), "ERROR")
            continue


logfile = "log/ANALYSIS_" + str(date.today()) + ".log"
logging.basicConfig(filename=logfile, level=logging.INFO,
                    format='%(asctime)s (%(name)s) ->  %(levelname)s :: %(message)s')
try:
    local_db_helper = LocalDBHelper()
    twitter_helper = TwitterHelper()
    datalake_helper = DatalakeHelper()
    dwh_helper = DataWarehouseHelper()
except Exception as e:
    log("Error occurred during Fetching Helpers initialization -> " + str(e), "ERROR")
    sys.exit(-1)

date = datetime.today().strftime('%Y-%m-%d')
for client in local_db_helper.get_active_clients():
    processing(client[1], client[2])
