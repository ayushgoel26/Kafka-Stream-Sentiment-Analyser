from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.ml.feature import StopWordsRemover, HashingTF, Tokenizer, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
sc = SparkContext('local')
spark = SparkSession(sc)

tweet_path = "Tweets.csv"
df = spark.read.option("header", "true").option("inferSchema", "true").csv(tweet_path)
airline_stats = df.filter(df.text. isNotNull())['text', 'airline_sentiment']
tokenizer = Tokenizer(inputCol="text", outputCol="words")
remover = StopWordsRemover(inputCol=tokenizer.getOutputCol(), outputCol="filtered_text")
hashingTF = HashingTF(inputCol=remover.getOutputCol(), outputCol="features")
indexer = StringIndexer(inputCol='airline_sentiment', outputCol="label")
lr = LogisticRegression(maxIter=10, regParam=0.001)
pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, indexer, lr])
paramGrid = ParamGridBuilder() \
    .addGrid(hashingTF.numFeatures, [5, 10, 100, 500, 1000]) \
    .addGrid(lr.regParam, [0.3, 0.1, 0.01, 0.05]) \
    .build()
crossval = CrossValidator(estimator=pipeline,
                          estimatorParamMaps=paramGrid,
                          evaluator=MulticlassClassificationEvaluator(),
                          numFolds=3)
cvModel = crossval.fit(airline_stats)
cvModel.save('trained_model')
