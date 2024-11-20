import sparknlp, sys

spark = sparknlp.start(real_time_output=True, gpu=True)
spark.sparkContext.setLogLevel('DEBUG')
print("Spark NLP version")
sparknlp.version()

train_dataset = spark.read.parquet("toxic_train.snappy.parquet").repartition(160)
test_dataset = spark.read.parquet("toxic_test.snappy.parquet").repartition(160)

# train_dataset.cache()

from pyspark.ml import Pipeline

from sparknlp.annotator import *
from sparknlp.common import *
from sparknlp.base import *

document = DocumentAssembler()\
  .setInputCol("text")\
  .setOutputCol("document")\
  .setCleanupMode("shrink")

embeddings = UniversalSentenceEncoder.pretrained() \
  .setInputCols(["document"])\
  .setOutputCol("sentence_embeddings")

pipeline = Pipeline(stages = [document, embeddings])
train_dataset = pipeline.fit(train_dataset).transform(train_dataset)
# train_embed.write.mode('overwrite').parquet('embeds/train')
test_dataset = pipeline.fit(test_dataset).transform(test_dataset)
# test_embed.write.mode('overwrite').parquet('embeds/test')
# sys.exit()

# train_dataset = spark.read.parquet('embeds/train').repartition(160)
# test_dataset = spark.read.parquet('embeds/test').repartition(160)
# print(train_dataset.rdd.getNumPartitions())

print('embed done')

multiClassifier = MultiClassifierDLApproach()\
  .setInputCols("sentence_embeddings")\
  .setOutputCol("category")\
  .setLabelColumn("labels")\
  .setBatchSize(128)\
  .setMaxEpochs(5)\
  .setLr(1e-3)\
  .setThreshold(0.5)\
  .setShufflePerEpoch(False)\
  .setEnableOutputLogs(True)\
  .setValidationSplit(0.1)

pipeline = Pipeline(stages=[multiClassifier])
pipelineModel = pipeline.fit(train_dataset)
pipelineModel.stages[-1].write().overwrite().save('tmp_multi_classifierDL_model')
print('preds')
preds = pipelineModel.transform(test_dataset)
preds = preds.select('labels','text',"category.result")
preds.repartition(160).cache()
print(preds.rdd.getNumPartitions())
preds.write.mode('overwrite').parquet("output")
print('done')
