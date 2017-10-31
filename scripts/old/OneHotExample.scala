import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}

val df = spark.createDataFrame(Seq(
  (0, "a"),
  (1, "b"),
  (2, "c"),
  (3, "d"),
  (4, "a"),
  (5, "c")
)).toDF("id", "category")
  

  

val indexer = new StringIndexer().setInputCol("category").setOutputCol("categoryIndex").fit(df)
val indexed = indexer.transform(df)

val encoder = new OneHotEncoder().setInputCol("categoryIndex").setOutputCol("categoryVec")

val encoded = encoder.transform(indexed)
encoded.show()
  
val df = spark.createDataFrame(Seq(
  (0, 9),
  (1, 2),
  (2, 2),
  (3, 1),
  (4, 9),
  (5, 2)
)).toDF("id", "category")
  
val encoder = new OneHotEncoder().setInputCol("category").setOutputCol("categoryVec")

val encoded = encoder.transform(df)
encoded.show()