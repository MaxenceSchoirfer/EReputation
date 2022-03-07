import logging
import sys
import threading

from analysis import analysis_helper
from api.twitter_helper import TwitterHelper
from data.dataset import Dataset
from helpers.datalake_helper import DatalakeHelper
from helpers.datamart_helper import DataMartHelper
from helpers.datawarehouse_helper import DataWarehouseHelper
from datetime import date, datetime

from database.local_db_helper import LocalDBHelper


def log(message, level):
    if True:
        print(message)
    if level == "INFO":
        logging.info(message)
    elif level == "ERROR":
        logging.error(message)


def processing(alias):
    # ----------------------------------------  DOWNLOAD FILES LOCALLY -------------------------------------------------------------
    try:
        message = "Start uploading files on the datalake [source : TWITTER, client : " + alias + ", date : " + date + "]"
        log(message, "INFO")

        files = datalake_helper.get_local_filenames(alias, date)
        # for file in files:
        #     datalake_helper.upload_file(file)

    except FileExistsError as e:
        message = "Error occurred during fetching files from datalake [source : TWITTER, client : " + alias + ", date : " + date + "] -> " + str(
            e)
        log(message, "ERROR")
        return

    # ------------------------------------- ANALYSIS ------------------------------------------------------------------

    for file in files:
        try:
            message = "Start analysis [file : " + file + ", source : TWITTER, client : " + alias + ", date : " + date + "]"
            log(message, "INFO")
            dataset = Dataset(file, False)
            analysis_helper.analysis(dataset)
            datasets.append(dataset)
        except FileExistsError as e:
            message = "Error occurred during analysis [file : " + file + ", source : TWITTER, client : " + alias + ", date : " + date + "] -> " + str(
                e)
            log(message, "ERROR")
            return



logfile = "log/ANALYSIS_" + str(date.today()) + ".log"
logging.basicConfig(filename=logfile, level=logging.INFO,
                    format='%(asctime)s (%(name)s) ->  %(levelname)s :: %(message)s')

try:
    local_db_helper = LocalDBHelper()
    twitter_helper = TwitterHelper()
    datalake_helper = DatalakeHelper()
    dwh_helper = DataWarehouseHelper()
except Exception as e:
    message = "Error occurred during Fetching Helpers initialization -> " + str(e)
    log(message, "ERROR")
    sys.exit(-1)

date = datetime.today().strftime('%Y-%m-%d')
threads = []
clients = []
datasets = []
for client in local_db_helper.get_active_clients():
    clients.append(client[1])
    alias = client[1]
    keyword = client[2]

    try:
        message = "Start fetching data from API [source : TWITTER, client : " + alias + ", date : " + date + "]"
        log(message, "INFO")
        twitter_helper.generate_csv(keyword, alias)
    except Exception as e:
        message = "Error occurred during fetching data from API [source : TWITTER, client : " + alias + ", date : " + date + "] -> " + str(
            e)
        log(message, "ERROR")

    threads.append(threading.Thread(target=processing, args=(client[1], )))

for t in threads:
    t.start()

for t in threads:
    t.join()

for dataset in datasets:
    try:
        message = "Start storage of analyse results [file : " + dataset.file + ", source : TWITTER, client : " + dataset.client + ", date : " + date + "]"
        log(message, "INFO")
        dwh_helper.save_dataset_analysis(dataset)
    except Exception as e:
        message = "Error occurred during storage of analyse results  [file : " + dataset.file + ", source : TWITTER, client : " + dataset.client + ", date : " + date + "] -> " + str(
            e)
        log(message, "ERROR")


