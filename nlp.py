from pyspark.sql import SparkSession, functions as f, types as t, Window
import sys

import sparknlp.pretrained
assert sys.version_info >= (3, 5)
import sparknlp
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession, functions as f, types as t, Window

from sparknlp.annotator import *
from sparknlp.common import *
from sparknlp.base import *

def main(ques):
    q_df = spark.read.parquet(ques)
    data = q_df.groupBy('Id').agg(
        f.first('Body').alias('body'),
        f.collect_list('Tag').alias('tags')
    )
    data = data.withColumn("tags", f.col("tags").cast(t.ArrayType(t.StringType(), True)))
    train, test = data.randomSplit([0.8, 0.2])
    test = test.repartition(160)
    train = train.repartition(160)
    train.cache()

    document = DocumentAssembler()\
        .setInputCol("body")\
        .setOutputCol("document")\
        .setCleanupMode("shrink")

    embeddings = UniversalSentenceEncoder.pretrained() \
        .setInputCols(["document"])\
        .setOutputCol("sentence_embeddings")
    
    test_pipeline = Pipeline(stages=[document, embeddings])
    tmp_test = test_pipeline.fit(test).transform(test)
    tmp_test.write.mode('overwrite').parquet('./nlp-embed/test')

    multiClassifier = MultiClassifierDLApproach()\
        .setInputCols("sentence_embeddings")\
        .setOutputCol("predicted_tags")\
        .setLabelColumn("tags")\
        .setBatchSize(128)\
        .setMaxEpochs(5)\
        .setLr(1e-3)\
        .setThreshold(0.5)\
        .setShufflePerEpoch(False)\
        .setEnableOutputLogs(True)\
        .setValidationSplit(0.1)\
        .setEvaluationLogExtended(True)\
        .setTestDataset("./nlp-embed/test")

    pipeline = Pipeline(
        stages = [
            document,
            embeddings,
            multiClassifier
        ])
    pipelineModel = pipeline.fit(train)
    pipelineModel.stages[-1].write().overwrite().save('tmp_multi_classifierDL_model')
    # print('preds')
    # preds = pipelineModel.transform(test)
    # preds = preds.select('tags','body',"predicted_tags.result")
    # preds.write.mode('overwrite').parquet("output-nlp/test")
    print('done')

if __name__ == '__main__':
    ques = sys.argv[1]
    spark = sparknlp.start(gpu=False)
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(ques)