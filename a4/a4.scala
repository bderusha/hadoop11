import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import org.jblas.DoubleMatrix

// object a4 {
    def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
      vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
    }
//   // arguments: hdfs:///shared3/data-small.txt hdfs:///shared3/items.txt
//   def main(args: Array[String]): Unit = {

    // Initialize Spark
    // val conf = new SparkConf().setAppName("Assignment4").setMaster(master)
    // val sc = new SparkContext(conf)

    // val reviews = sc.textFile(args(0))
    // val items = sc.textFile(args(1))
    val reviews = sc.textFile("hdfs:///shared3/data-small.txt")
    println("reviews")
    val items = sc.textFile("hdfs:///shared3/items.txt")
    println("items")

    // Take a very small dataset for now as a sample data set
    // In practice we will use the whole file
    // val tinyData = reviews.take(1100)

    // Filter down to only the productId, userId, and score
    val filtered_data = reviews.filter(
      s => (
        !s.isEmpty() && (s.startsWith("product/productId") || s.startsWith("review/userId: ") || s.startsWith("review/score: "))
        )
      ).map(_.split(": ")(1))

    // Take every 3 elements and group them together.  List(Array(pid, uid, score ), Array(id, uid, score))
    val grouped_data = filtered_data.collect().grouped(3).toList

    // Then group those elements by productId
    // Map(
    //      pid1 -> List(Array(pid1, uid, score), Array(pid1, uid, score), ),
    //      pid2 -> List(Array(pid2, uid, score)),
    // )
    // And filter out any products with only one review to save memory
    val multi_entry_data = grouped_data.groupBy(_(0)).filter((x) => x._2.length > 1).flatMap{ case (k,v) => v }.toList

    // We will need a list of just product and user ids so we have an Int index to pass to the recommender
    val productIds = multi_entry_data.map(_(0)).distinct
    // (k,v) amazonID => index
    val productMap = (productIds zip productIds.indices).toMap
    // (k,v) index => amazonID
    val reverseProductMap = productMap.map(_.swap)
    // User ids are only necessary for creating the integer ids
    val userIds = multi_entry_data.map(_(1)).distinct
    val userMap = (userIds zip userIds.indices).toMap
    println("maps created")

    // val ratings = multi_entry_data.map { case Array(productId, userId, rating) => Rating(userIds.indexOf(userId), productIds.indexOf(productId), rating.toDouble) }
    val ratings = multi_entry_data.map { case Array(productId, userId, rating) => Rating(userMap(userId), productMap(productId), rating.toDouble) }

    // COMMENT: I don't think we need to split create a training and test set, since there is no way of confirming
    // the correctness of our recommendations on the test set.
    // val ratingsDF = ratings.toDF()
    // val Array(training, test) = ratingsDF.randomSplit(Array(0.8, 0.2))
    // val trainingRDD = sc.parallelize(training)

    // Build the recommendation model using ALS
    val rank = 10
    val numIterations = 10
    val ratingsRDD = sc.parallelize(ratings)
    val model = ALS.train(ratingsRDD, rank, numIterations, 0.01)

    val N = 10;

    val recommendations  = productIds.map{baseId =>
    //   val baseIdInt = productIds.indexOf(baseId)
      val baseIdInt = productMap(baseId)
      // Lookup the feature array of the item represented as Array[Double]
      val baseItemFactor = model.productFeatures.lookup(baseIdInt).head
      // Put feature array in a vector (or one dimensional DoubleMatrix in jblas)
      val baseItemVector = new DoubleMatrix(baseItemFactor)
      // Find how similar the base item is to every item
      val similarities: RDD[(String, Double)]= model.productFeatures.map{ case (testIdInt, testItemFactor) =>
        // Get features of the test item as a vector
        val testItemVector = new DoubleMatrix(testItemFactor)
        // Compute a similarity score between the two vectors using cosine similarity
        val similarity = cosineSimilarity(baseItemVector, testItemVector)
        // Output a tuple of the test item and its similarity score with the base item
        // val testId = productIds(testIdInt)
        val testId = reverseProductMap(testIdInt)
        (testId, similarity)
      }
      // First, the base item itself will be included with top similarity score of 1 so exclude this
      val recs = similarities.filter{ case(testId, similarity) => baseId != testId}
        // Order by descending order of similarity score and get the top N elements
        .top(N)(Ordering.by[(String, Double), Double] {case (testId, similarity) => similarity})
        // Map each of the top N elements to its itemId
        .map {case(testId, similarity) => testId}
        // Collect recommendations to list
        .toList
      // Final output: <Product>,<Recommended Product>,<Recommended Product>,...
      val output = baseId + "," + recs.mkString(",")

      output
    }

    sc.parallelize(recommendations).saveAsTextFile("/user/hadoop11/a4results/test-medium1")
    println("print")
    // Given the feature vectors of two items, return a double similarity score.
    // Compute the dot product of the two vectors and divide by the product of the lengths.
//   }
//
//
// }
