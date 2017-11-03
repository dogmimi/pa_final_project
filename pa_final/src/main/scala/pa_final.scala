import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.feature.{StandardScaler, StandardScalerModel}
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import math._
import java.util.Calendar
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.Statistics

object pa_final {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("PA Final"))

    //train
    val trainRdd = sc.textFile("hdfs:/user/hc2264/PA/train.csv")
    //val trainRdd = sc.textFile("hdfs:/user/hc2264/PA/train.sample.csv")
    val trainRddHeader = trainRdd.first()
    val trainRddClean1 = trainRdd.filter(line => line != trainRddHeader)

    //transaction
    val transactionRdd = sc.textFile("hdfs:/user/hc2264/PA/transactions.csv")
    //val transactionRdd = sc.textFile("hdfs:/user/hc2264/PA/transactions.sample.csv")
    val transactionRddHeader = transactionRdd.first()
    val transactionRddClean1 = transactionRdd.filter(line => line != transactionRddHeader)

    //user_logs
    val userLogsRdd = sc.textFile("hdfs:/user/hc2264/PA/user_logs.csv")
    //val userLogsRdd = sc.textFile("hdfs:/user/hc2264/PA/user_logs.sample.csv")
    val userLogsRddHeader = userLogsRdd.first()
    val userLogsRddClean1 = userLogsRdd.filter(line => line != userLogsRddHeader)

    //members
    val membersRdd = sc.textFile("hdfs:/user/hc2264/PA/members.csv")
    //val membersRdd = sc.textFile("hdfs:/user/hc2264/PA/members.sample.csv")
    val membersRddHeader = membersRdd.first()
    val membersRddClean1 = membersRdd.filter(line => line != membersRddHeader)

    //check how many users' age are valid
    val membersRddClean3 = membersRddClean1.filter{line => val data = line.split(",")
      if(data(2) == null || data(2).toInt < 3 || data(2).toInt > 120)
        false
      else
        true
    }

    val membersRddMale = membersRddClean1.filter{line => val data = line.split(",")
      if(data(3) == "male")
        true
      else
        false
    }
    val membersRddFemale = membersRddClean1.filter{line => val data = line.split(",")
      if(data(3) == "female")
        true
      else
        false
    }

    val maleAvgAge = membersRddClean3.filter{line => val data = line.split(",")
      if(data(3) == "male")
        true
      else
        false
    }.map(line => line.split(",")(2).toDouble)
    val maleAge = maleAvgAge.reduce(_ + _) / maleAvgAge.count()

    val femaleAvgAge = membersRddClean3.filter{line => val data = line.split(",")
      if(data(3) == "female")
        true
      else
        false
    }.map(line => line.split(",")(2).toDouble)
    val femaleAge = femaleAvgAge.reduce(_ + _) / femaleAvgAge.count()


    //print information
    var trainRddCount = trainRddClean1.count()
    println("train rdd count = " + trainRddCount)
    var transactionRddCount = transactionRddClean1.count()
    println("transaction rdd count = " + transactionRddCount)
    var userLogsRddCount = userLogsRddClean1.count()
    println("user logs rdd count = " + userLogsRddCount)
    var membersRddCount = membersRddClean1.count()
    println("members rdd count = " + membersRddCount)
    var membersRddCountValid = membersRddClean3.count()
    println("members rdd count after removing outlier - age = " + membersRddCountValid)
    var membersRddCountNotValid = membersRddCount - membersRddCountValid
    println("members rdd count - not valid = " + membersRddCountNotValid)
    var maleCount = membersRddMale.count()
    var femaleCount = membersRddFemale.count()
    var noGenderCount = membersRddCount - maleCount - femaleCount
    println("male members rdd count = " + maleCount)
    println("female members rdd count = " + femaleCount)
    println("members without gender count = " + noGenderCount)
    println("male avg age = " + maleAge)
    println("female avg age = " + femaleAge)
  
    sc.stop()
  }
}
