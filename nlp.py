from sparknlp.base import *
from sparknlp.common import *
from sparknlp.annotator import *
import sparknlp.pretrained
import sparknlp
from pyspark.sql import functions as f, types as t
from pyspark.ml import Pipeline
import sys
import shutil
assert sys.version_info >= (3, 5)


def main(ques, model_dir):
    q_df = spark.read.parquet(ques)
    data = q_df.groupBy('Id').agg(
        f.first('Body').alias('body'),
        f.collect_list('Tag').alias('tags')
    )
    data = data.withColumn("tags", f.col("tags").cast(
        t.ArrayType(t.StringType(), True))).repartition(160)
    train, test = data.randomSplit([0.8, 0.2])
    test.cache()

    document = DocumentAssembler()\
        .setInputCol("body")\
        .setOutputCol("document")\
        .setCleanupMode("shrink")

    embeddings = UniversalSentenceEncoder.pretrained() \
        .setInputCols(["document"])\
        .setOutputCol("sentence_embeddings")

    tmp_embedded_test_dir = './embedded-test-data'
    test_pipeline = Pipeline(stages=[document, embeddings])
    tmp_test = test_pipeline.fit(test).transform(test)
    tmp_test.write.mode('overwrite').parquet(tmp_embedded_test_dir)

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
        .setTestDataset(tmp_embedded_test_dir)

    pipeline = Pipeline(
        stages=[
            document,
            embeddings,
            multiClassifier
        ])
    model = pipeline.fit(train)
    model.stages[-1].write().overwrite().save(model_dir)
    # preds = pipelineModel.transform(test)
    # preds = preds.select('tags','body',"predicted_tags.result")
    # preds.write.mode('overwrite').parquet("output-nlp/test")
    shutil.rmtree(tmp_embedded_test_dir)
    print('NLP Complete.')


if __name__ == '__main__':
    ques = sys.argv[1]
    model_dir = sys.argv[2]
    spark = sparknlp.start(gpu=False)
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(ques, model_dir)
