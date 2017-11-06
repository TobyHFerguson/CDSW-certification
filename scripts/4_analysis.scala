import org.apache.spark.sql.{DataFrame,Dataset}
import org.apache.spark.ml.classification.{LogisticRegression, RandomForestClassifier,GBTClassifier};
import org.apache.spark.sql.functions._;
import org.apache.spark.ml.feature._;
import org.apache.spark.ml.linalg.DenseMatrix;
import org.apache.spark.ml._;

// Read in the parquet file

val brfss = spark.read.format("parquet").load("brfss/2011.parquet", "brfss/2012.parquet", "brfss/2013.parquet", "brfss/2014.parquet", "brfss/2015.parquet", "brfss/2016.parquet").cache();
  
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
                                                                      
                            
val splits = brfss_transformed.randomSplit(Array(0.75, 0.25),12345) ;                                                                         
val training = splits(0).cache();
val test = splits(1).cache();
  

def confusion_matrix(prediction: DataFrame) = {
  val p = prediction
  val tp = p.filter("label == 1 and prediction ==1").count;
  val fp = p.filter("label == 0 and prediction ==1").count;
  val tn = p.filter("label == 0 and prediction ==0").count;
  val fn = p.filter("label == 1 and prediction == 0").count;
                                       
  new DenseMatrix(2,2,Array(tn, fn, fp, tp))
}

def errorRate(prediction: DataFrame) = {
  prediction.filter("label != prediction").count.toDouble / prediction.count
}

val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8);  
  // Fit the model
val lrModel : Transformer = lr.fit(training)
val lrPrediction = lrModel.transform(test).cache()

// Logistic Regression Evaluation
//Confusion Matrix - predictions in columns, non-asthma | asthma
println(confusion_matrix(lrPrediction))
//Error rate
println("%.2f%%".format(errorRate(lrPrediction)*100))


// Random Forest
val rf = new RandomForestClassifier().setNumTrees(10)   
val rfModel = rf.fit(training)
val rfPrediction = rfModel.transform(test).cache()
//
//Random Forest Evaluation
//Confusion Matrix - predictions in columns, non-asthma | asthma
println("\n"+confusion_matrix(lrPrediction))
//Error Rate
println("\nError Rate: %.2f%%".format(errorRate(lrPrediction)*100))
  

// # Gradient Boosted Trees
val gbt = new GBTClassifier().setMaxIter(10)   
val gbtModel = gbt.fit(training)
val gbtPrediction = gbtModel.transform(test).cache()

//Gradient Boosted Trees Evaluation
//Confusion Matrix - predictions in columns, non-asthma | asthma
println("\n"+confusion_matrix(gbtPrediction))
//Error Rate
println("\nError Rate: %.2f%%".format(errorRate(gbtPrediction)*100))

// The results demonstrate that the model simply chooses 'no asthma' as the best predictor, no
// matter what.

// This could be because there are very few asthma cases in the data. In the training data the ratio 
// between asthmatics and non-asthmatics is:
training.filter("label=1").count()/training.filter("label=0").count().toFloat*100.0.round


// To get round this we'll try boosting the positive (has asthma) result ratio (by reducing the number
// of non-asthma samples) and see if anything that improves things!
val training_boosted = training.filter("label=1").union(training.filter("label = 0").sample(true, 0.4,3839))
val tbModel = lr.fit(training_boosted)
val tbp = tbModel.transform(test)

// Training Boosted Evaluation
// Confusion Matrix
println("\n"+confusion_matrix(tbp))
//Error Rate
println("\nError Rate: %.2f%%".format(errorRate(tbp)*100))


// The result is the same, disappointingly.

// I think we're done for now

// # Conclusion
// No meaningful prediction regarding asthmatic status can be made from the given dataset using the following
// features:
// * RFHLTH
// * HCVU651
// * MICHD
// * DRDXAR1
// * CVDINFR4
// * CVDCRHD4
// * RACEGR3
// * AGE_G
// * BMI5CAT
// * SMOKER3

