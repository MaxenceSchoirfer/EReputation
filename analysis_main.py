import analysis.global_helper as analyse
import os

# root.analysis("../data/twitter/_TWITTER_COCA_2022-10-01_UNK_EN.csv", False, 1)
# simp_path = '../data/test/covid.csv'
# abs_path = os.path.abspath(simp_path)
# print(abs_path)
# root.analysis(abs_path, True)
#
# simp_path = '../data/test/test.csv'
# abs_path = os.path.abspath(simp_path)
# print(abs_path)
# root.analysis(abs_path, True)

simp_path = '../data/test/_TWITTER_COCA_2022-10-01_UNK_EN.csv'
abs_path = os.path.abspath(simp_path)
print(abs_path)
analyse.analysis(abs_path, False)
