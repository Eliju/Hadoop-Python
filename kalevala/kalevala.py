export PYTHONIOENCODING=utf8
pyspark


import pyspark.sql.functions as F
from pyspark.sql.functions import desc

text = spark.read.text("hdfs:///datasets/text/kalevala/1/kalevala_utf8.txt")
text_filtered = text.filter(~text.value.contains("*")).filter(text.value!=F.upper(text.value))
text_final = text_filtered.filter(~text.value.contains("apologize")).filter(~text.value.contains("we")).filter(~text.value.contains("will")).filter(~text.value.contains("Project")).filter(~text.value.contains("P r o j e c t")).filter(~text.value.contains("electronic"))
wordsRDD = text_final.rdd.flatMap(lambda x: x[0].split(" "))
words2RDD = wordsRDD.map(lambda x: x.replace(",","").replace(".","").replace('"','').replace("!","").replace("?","").replace(":",""))
words3RDD = words2RDD.filter(lambda x: len(x) > 0).map(lambda x: x[1:] if x[0] == "'" else x[0:])
wordRDD = words3RDD.map(lambda x:(x,1))
wordcount = wordRDD.reduceByKey(lambda a,b: a+b)
dF_wordcount = spark.createDataFrame(wordcount)
dF_sortedByKey = dF_wordcount.orderBy(["_1", "_2"], ascending=[1,-1])
dF_sortedByCount = dF_wordcount.orderBy(desc("_2"))

dF_sortedByKey.coalesce(1).write.csv('/datasets/text/kalevala/1/KalevalanSanatAakkostus.csv')
dF_sortedByCount.coalesce(1).write.csv('/datasets/text/kalevala/1/KalevalanSanatYleisyys.csv')