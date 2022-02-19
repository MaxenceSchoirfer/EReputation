# get acive users -> from ??? DWH bad practice
from helpers import datalake_helper
from helpers.datalake_helper import DatalakeHelper
from analysis import analysis

#probably have to use thread
# foreach active users
# call twitter API
# download csv files

datalake = DatalakeHelper()
client_alias = "COCA"
datalake.get_files()
filenames = datalake.get_filenames("data/" + client_alias + "/")
for file in filenames:
    analysis.analysis(file, True, 0, False)

