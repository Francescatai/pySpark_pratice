import jieba
from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel
from jieba_defs import context_jieba,filter_words,append_words,extract_user_and_word
from operator import add

if __name__ == '__main__':
    # jieba測試
    """
    content = "這是測試用的一句話"
    # result = jieba.cut(content, False)
    # print(list(result))
    # print(type(result))
    # result2 = jieba.cut_for_search(content)
    # print("/".join(result2))
    """
#     初始化執行環境，構建SparkContext對象
    conf = SparkConf().setAppName("test")
    sc = SparkContext(conf=conf)
#     讀取數據
    file_rdd = sc.textFile("hdfs://node1:8020/input/searchdata.txt")
#     切分數據
    spilt_rdd = file_rdd.map(lambda x:x.split("\t"))
#     因為要做多個需求，spilt_rdd會被多次使用，緩存spilt_rdd
    spilt_rdd.persist(StorageLevel.DISK_ONLY)

"""需求1:user search關鍵詞分析"""
#       step1:隨機取出search內容數據
spilt_rdd.takeSample(True,3)
context_rdd = spilt_rdd.map(lambda x:x[2])
#       step2:對內容進行分詞分析
words_rdd = context_rdd.flatMap(context_jieba)
#       step3:過濾停用詞
filter_rdd = words_rdd.filter(filter_words)
#       step4:修訂關鍵詞內容
final_words_rdd = filter_rdd.map(append_words)
#       step5:對數據進行分組聚合排序，算出前5名
result1 = final_words_rdd.reduceByKey(lambda a, b:a+b).sortBy(lambda x:x[1],ascending=False,numPartitions=1 ).take(5)
# print("排名前五為"+result1)

"""需求2:user & user search組合分析"""
#       step1:把內容進行分詞後跟user id組合
user_content_rdd = spilt_rdd.map(lambda x:(x[1],x[2]))
user_word_rdd = user_content_rdd.flatMap(extract_user_and_word)
#       step2:對數據進行分組聚合排序，算出前5名
result2 = user_word_rdd.reduceByKey(add).sortBy(lambda x:x[1],ascending=False,numPartitions=1 ).take(5)
# print("用戶及其感興趣的內容為:"result2)
