import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.sql.functions._;
import org.apache.spark.ml.feature._;
import org.apache.spark.ml._;

  
val brfss = spark.read.format("parquet").load("brfss/2011.parquet");
  
//Drop the null valued rows
val brfss_no_nulls = brfss.na.drop().filter("CASTHM1 in (1,2)");
  


// Function to convert the 1, 2, to a yes/no/otherwise string  
def yes_no(arg : Int) : String = { 
    arg match {
      case 1 => "Yes";
      case 2 => "No";
      case _ => "unknown"

    }
}
//Function to encode the race group
def code_racegroup(racegroup: Int) : String = { racegroup match { case 1 => "whitenonhispanic"; case 2 => "blacknonhispanic"; case 3 => "othernonhispanic"; case 4 => "multiracialnonhispanic"; case 5 => "hispanic"; case _ => "unknown" } }
def code_agegroup(agegroup: Int) : String = { agegroup match { case 1 => "18+"; case 2 => "25+"; case 3 => "35+"; case 4 => "45+"; case 5 => "55+"; case 6 => "65+"; case _ => "unknown" } }
def code_bmicategory(bmicategory: Int) : String = { bmicategory match { case 1 => "underweight"; case 2 => "normalweight"; case 3 => "overweight"; case 4 => "obese";  case _ =>"unknown" } }
def code_smoker(smoker: Int) : String = { smoker match { case 1 => "everyday"; case 2 => "somedays"; case 3 => "former"; case 4 => "never"; case _ => "unknown" } }



val yes_no_udf = udf(yes_no _);
val code_racegroup_udf = udf(code_racegroup _);
val code_agegroup_udf = udf(code_agegroup _);
val code_bmicategory_udf = udf(code_bmicategory _);
val code_smoker_udf = udf(code_smoker _);
  
  
val brfss_labeled = brfss_no_nulls.withColumn("label", 'CASTHM1 - 1)
val brfss_strings = brfss_labeled.
  withColumn("ingoodhealth", yes_no_udf('RFHLTH)).
  withColumn("hascoverage", yes_no_udf('HCVU651)).
  withColumn("hasheartproblem", yes_no_udf('MICHD)).
  withColumn("hasarthritis", yes_no_udf('DRDXAR1)).
  withColumn("hadheartattack", yes_no_udf('CVDINFR4)).
  withColumn("hadangina", yes_no_udf('CVDCRHD4)).
  withColumn("racegroup", code_racegroup_udf('RACEGR3)).
  withColumn("agegroup", code_agegroup_udf('AGE_G)).
  withColumn("bmicategory", code_bmicategory_udf('BMI5CAT)).
  withColumn("smoker", code_smoker_udf('SMOKER3)) ;                                                                 
                                   

val ingoodhealth_idxr = new StringIndexer().setInputCol("ingoodhealth").setOutputCol("ingoodhealth_idx");
val ingoodhealth_ohr = new OneHotEncoder().setInputCol("ingoodhealth_idx").setOutputCol("ingoodhealth_ohe");
val hascoverage_idxr = new StringIndexer().setInputCol("hascoverage").setOutputCol("hascoverage_idx");
val hascoverage_ohr = new OneHotEncoder().setInputCol("hascoverage_idx").setOutputCol("hascoverage_ohe");
val hasheartproblem_idxr = new StringIndexer().setInputCol("hasheartproblem").setOutputCol("hasheartproblem_idx");
val hasheartproblem_ohr = new OneHotEncoder().setInputCol("hasheartproblem_idx").setOutputCol("hasheartproblem_ohe");
val hasarthritis_idxr = new StringIndexer().setInputCol("hasarthritis").setOutputCol("hasarthritis_idx");
val hasarthritis_ohr = new OneHotEncoder().setInputCol("hasarthritis_idx").setOutputCol("hasarthritis_ohe");
val hadheartattack_idxr = new StringIndexer().setInputCol("hadheartattack").setOutputCol("hadheartattack_idx");
val hadheartattack_ohr = new OneHotEncoder().setInputCol("hadheartattack_idx").setOutputCol("hadheartattack_ohe");
val hadangina_idxr = new StringIndexer().setInputCol("hadangina").setOutputCol("hadangina_idx");
val hadangina_ohr = new OneHotEncoder().setInputCol("hadangina_idx").setOutputCol("hadangina_ohe");
val racegroup_idxr = new StringIndexer().setInputCol("racegroup").setOutputCol("racegroup_idx");
val racegroup_ohr = new OneHotEncoder().setInputCol("racegroup_idx").setOutputCol("racegroup_ohe");
val agegroup_idxr = new StringIndexer().setInputCol("agegroup").setOutputCol("agegroup_idx");
val agegroup_ohr = new OneHotEncoder().setInputCol("agegroup_idx").setOutputCol("agegroup_ohe");
val bmicategory_idxr = new StringIndexer().setInputCol("bmicategory").setOutputCol("bmicategory_idx");
val bmicategory_ohr = new OneHotEncoder().setInputCol("bmicategory_idx").setOutputCol("bmicategory_ohe");
val smoker_idxr = new StringIndexer().setInputCol("smoker").setOutputCol("smoker_idx");
val smoker_ohr = new OneHotEncoder().setInputCol("smoker_idx").setOutputCol("smoker_ohe");
                                       

                                       
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
        "smoker_ohe")).
   setOutputCol("features"); 
                                       
val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8);

val transform_pipeline = new Pipeline().setStages(
  Array(ingoodhealth_idxr,
        ingoodhealth_ohr,
        hascoverage_idxr,
        hascoverage_ohr,
        hasheartproblem_idxr,
        hasheartproblem_ohr,
        hasarthritis_idxr,
        hasarthritis_ohr,
        hadheartattack_idxr,
        hadheartattack_ohr,
        hadangina_idxr,
        hadangina_ohr,
        racegroup_idxr,
        racegroup_ohr,
        agegroup_idxr,
        agegroup_ohr,
        bmicategory_idxr,
        bmicategory_ohr,
        smoker_idxr,
        smoker_ohr,
        assembler));
                                       
val brfss_transformer = transform_pipeline.fit(brfss_strings);
val brfss_transformed = brfss_transformer.transform(brfss_strings);
                                       

                                       
                            
val splits = brfss_transformed.randomSplit(Array(0.75, 0.25),12345) ;                                                                         
val training = splits(0).cache();
val test = splits(1).cache();
  
                                    

  
  // Fit the model
val lrModel = lr.fit(training);
// make a prediction
val p = lrModel.transform(test)
                                       
val tp = p.filter("label == 1 and prediction ==1").count;
val fp = p.filter("label == 0 and prediction ==1").count;
val tn = p.filter("label == 0 and prediction ==0").count;
val fn = p.filter("label == 1 and prediction == 0").count;
                                       
spark.createDataFrame(Seq(
 ("Predicted +", tp, fp),
  ("Predicted -",fn, tn)
)).toDF("","Actual +", " Actual -").show()
                                       
                                      

