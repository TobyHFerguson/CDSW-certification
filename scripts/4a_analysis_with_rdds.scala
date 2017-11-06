// This analysis is similar to the other scala analysis except using 
// rdds directly (i.e. the (now deprecated) mllib interface). This is because
// mllib includes analysis classes such as MulticlassMetrics which are unavailable
// in the newer, and incomplete, ml.

import org.apache.spark.sql.{DataFrame,Dataset}
import org.apache.spark.ml.classification.{LogisticRegression, RandomForestClassifier,GBTClassifier};
import org.apache.spark.sql.functions._;
import org.apache.spark.ml.feature._;
import org.apache.spark.ml._;

// Read in the parquet file

//val brfss = spark.read.format("parquet").load("brfss/2011.parquet", "brfss/2012.parquet", "brfss/2013.parquet", "brfss/2014.parquet", "brfss/2015.parquet", "brfss/2016.parquet").cache();
val brfss = spark.read.format("parquet").load("brfss/2011.parquet").cache();
  
// Drop the null valued rows and those with no asthma value.
val brfss_no_nulls = brfss.na.drop().filter("CASTHM1 in (1,2)");
  
// Add a 'label' column, using the value of 'CASTHM1', reducing the value to the range [0,1] (no asthma, asthma)
val brfss_labeled = brfss_no_nulls.withColumn("label", 'CASTHM1 - 1)
                                                            

val brfss_strings = brfss_labeled.
  withColumnRenamed("RFHLTH","ingoodhealth").
  withColumnRenamed("HCVU651","hascoverage").
  withColumnRenamed("MICHD","hasheartproblem").
  withColumnRenamed("DRDXAR1","hasarthritis").
  withColumnRenamed("CVDINFR4","hadheartattack").
  withColumnRenamed("CVDCRHD4","hadangina").
  withColumnRenamed("RACEGR3","racegroup").
  withColumnRenamed("AGE_G","agegroup").
  withColumnRenamed("BMI5CAT","bmicategory").
  withColumnRenamed("SMOKER3","smoker").
  withColumnRenamed("EDUCAG", "education").
  withColumnRenamed("INCOMG", "income").
  withColumnRenamed("RENTHOM1", "home_owner").
  withColumnRenamed("SEX", "sex").
  withColumnRenamed("STATE", "state");

                                       
val ingoodhealth_ohr = new OneHotEncoder().setInputCol("ingoodhealth").setOutputCol("ingoodhealth_ohe");
val hascoverage_ohr = new OneHotEncoder().setInputCol("hascoverage").setOutputCol("hascoverage_ohe");
val hasheartproblem_ohr = new OneHotEncoder().setInputCol("hasheartproblem").setOutputCol("hasheartproblem_ohe");
val hasarthritis_ohr = new OneHotEncoder().setInputCol("hasarthritis").setOutputCol("hasarthritis_ohe");
val hadheartattack_ohr = new OneHotEncoder().setInputCol("hadheartattack").setOutputCol("hadheartattack_ohe");
val hadangina_ohr = new OneHotEncoder().setInputCol("hadangina").setOutputCol("hadangina_ohe");
val racegroup_ohr = new OneHotEncoder().setInputCol("racegroup").setOutputCol("racegroup_ohe");
val agegroup_ohr = new OneHotEncoder().setInputCol("agegroup").setOutputCol("agegroup_ohe");
val bmicategory_ohr = new OneHotEncoder().setInputCol("bmicategory").setOutputCol("bmicategory_ohe");
val smoker_ohr = new OneHotEncoder().setInputCol("smoker").setOutputCol("smoker_ohe");
val education_ohr = new OneHotEncoder().setInputCol("education").setOutputCol("education_ohe");
val income_ohr = new OneHotEncoder().setInputCol("income").setOutputCol("income_ohe");
val home_owner_ohr = new OneHotEncoder().setInputCol("home_owner").setOutputCol("home_owner_ohe");
val sex_ohr = new OneHotEncoder().setInputCol("sex").setOutputCol("sex_ohe");
val state_ohr = new OneHotEncoder().setInputCol("state").setOutputCol("state_ohe");
                                       

                                       
 val assembler = new VectorAssembler().setInputCols(
   Array("ingoodhealth_ohe",
        "hascoverage_ohe",
        "hasheartproblem_ohe",
        "hasarthritis_ohe",
        "hadheartattack_ohe",
        "hadangina_ohe",
        "racegroup_ohe",
        "agegroup_ohe",
        "bmicategory_ohe",
        "smoker_ohe",
        "education_ohe",
        "income_ohe",
        "home_owner_ohe",
        "sex_ohe",
        "state_ohe"
        )).
   setOutputCol("features"); 
                                       


val transform_pipeline = new Pipeline().setStages(
  Array(
        ingoodhealth_ohr,
        hascoverage_ohr,
        hasheartproblem_ohr,
        hasarthritis_ohr,
        hadheartattack_ohr,
        hadangina_ohr,
        racegroup_ohr,
        agegroup_ohr,
        bmicategory_ohr,
        smoker_ohr,
        education_ohr,
        income_ohr,
        home_owner_ohr,
        sex_ohr,
        state_ohr,
        assembler
  ));
                                       
val brfss_transformer = transform_pipeline.fit(brfss_strings);
val brfss_transformed = brfss_transformer.transform(brfss_strings);
val brfss_lf = brfss_transformed.select("label", "features").cache()

import org.apache.spark.mllib.util.MLUtils

// I want to convert this into an RDD[LabeledPoint]
// If I convert to an rdd I'll have this type:
// RDD[Row[Int, org.apache.spark.ml.linalg.SparseVector]]
// However this is the new ml SpaseVector type - I want the older 
// mllib SparseVector type. So I need to convert the vector columns thus:
val brfss_rdd = MLUtils.convertVectorColumnsFromML(brfss_lf).rdd.cache();

// Here's how I figured out all the types - this took about 2 hours!
//brfss_rdd.take(1).getClass
//class [Lorg.apache.spark.sql.Row;

//brfss_rdd.take(1)(0).getClass
//class org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

//brfss_rdd.take(1)(0).schema
//StructType(StructField(label,IntegerType,true), StructField(features,org.apache.spark.ml.linalg.VectorUDT@3bfc3ba7,true))

//brfss_rdd.take(1)(0)(0).getClass
//class java.lang.Integer

//brfss_rdd.take(1)(0)(1).getClass
//class org.apache.spark.ml.linalg.SparseVector

// This bit is copied straight from the Spark user docs for LogisticRegression

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.sql.Row
val brfss_lp = brfss_rdd.map {case Row(k: Int, v: SparseVector) => new LabeledPoint(k, v)}

val splits = brfss_lp.randomSplit(Array(0.6, 0.4), seed = 11L)
val training = splits(0).cache()
val test = splits(1).cache()

// Run training algorithm to build the model
import org.apache.spark.mllib.classification._
val lr_model = new LogisticRegressionWithLBFGS().run(training)

// Compute raw scores on the test set.
val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
  val prediction = lr_model.predict(features)
  (prediction, label)
}

def getError(rdd: RDD[(Double, Double)]): Double = {
  rdd.filter(r => r._1 != r._2).count.toDouble / rdd.count
}

// Get evaluation metrics.
import org.apache.spark.mllib.evaluation._


// The confusion matrix (predicted values in columns, not asthma, then asthma)
// Shows that, largely, this model predicts not asthma in almost all cases
var cm = new MulticlassMetrics(predictionAndLabels).confusionMatrix
println("Logistic Regression Evaluation \n")
println("Confusion Matrix - predictions in columns, non-asthma | asthma")
println(cm)

//Furthermore the error rate is low, as one might expect:
println("Error rate: %.2f%%".format(getError(predictionAndLabels)*100))


// Now for a RandomForest.

// // Train a RandomForest model.

val numClasses = 2
// Ensure all features are categorical and binary
val categoricalFeaturesInfo: Map[Int, Int] = (0 to brfss_lp.take(1)(0).features.size - 1).map(a => a -> 2)(collection.breakOut)
val numTrees = 10 // Use more in practice.
val featureSubsetStrategy = "auto" // Let the algorithm choose.
val impurity = "gini"
val maxDepth = 4
val maxBins = 32

import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
val rf_model = RandomForest.trainClassifier(training, numClasses, categoricalFeaturesInfo,
  numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

// Evaluate model on test instances and compute test error
val predictionAndLabels = test.map { point =>
  val prediction = rf_model.predict(point.features)
  (prediction, point.label )
}
var cm = new MulticlassMetrics(predictionAndLabels).confusionMatrix
println("RandomForest Regression Evaluation \n")
println("Confusion Matrix - predictions in columns, non-asthma | asthma")
println(cm)
println("Error rate: %.2f%%".format(getError(predictionAndLabels)*100))


// Train a GradientBoostedTrees model.
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
// The defaultParams for Classification use LogLoss by default.
val boostingStrategy = BoostingStrategy.defaultParams("Classification")
boostingStrategy.numIterations = numTrees
boostingStrategy.treeStrategy.numClasses = numClasses
boostingStrategy.treeStrategy.maxDepth = maxDepth
boostingStrategy.treeStrategy.categoricalFeaturesInfo = categoricalFeaturesInfo

val gbt_model = GradientBoostedTrees.train(training, boostingStrategy)
// Evaluate model on test instances and compute test error
val predictionAndLabels = test.map { point =>
  val prediction = gbt_model.predict(point.features)
  (prediction, point.label )
}

var cm = new MulticlassMetrics(predictionAndLabels).confusionMatrix
println("Gradient Boosted Tree Evaluation \n")
println("Confusion Matrix - predictions in columns, non-asthma | asthma")
println(cm)
println("Error rate: %.2f%%".format(getError(predictionAndLabels)*100))

// So the conclusion is that none of these models do any better than those setup using 
// ml.

// We can see the trees:

// RandomForest:
println(rf_model.toDebugString)

// Gradient Boosted Trees
println(gbt_model.toDebugString)