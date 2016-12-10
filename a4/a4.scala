import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating

val reviews = sc.textFile("hdfs:///shared3/data-small.txt")
val items = sc.textFile("hdfs:///shared3/items.txt")

// Take a very small dataset for now as a sample data set
// In practice we will use the whole file
val tinyData = reviews.take(110)

// Filter down to only the productId, userId, and score
val filtered_data = tinyData.filter(
    s => (
        !s.isEmpty() && (s.startsWith("product/productId") || s.startsWith("review/userId: ") || s.startsWith("review/score: "))
    )
).map(_.split(": ")(1))

// Take every 3 elements and group them together.  List(Array(pid, uid, score ), Array(id, uid, score))
val grouped_data = filtered_data.grouped(3).toList

// Then group those elements by productId
// Map(
//      pid1 -> List(Array(pid1, uid, score), Array(pid1, uid, score), ),
//      pid2 -> List(Array(pid2, uid, score)),
// ) 
// And filter out any products with only one review to save memory
val multi_entry_data = grouped_data.groupBy(_(0)).filter((x) => x._2.length > 1).flatMap{ case (k,v) => v }.toList

// We will need a list of just product and user ids so we have an Int index to pass to the recommender
val productIds = multi_entry_data.map(_(0)).distinct
val userIds = multi_entry_data.map(_(1)).distinct

val ratings = multi_entry_data.map { case Array(productId, userId, rating) => Rating(userIds.indexOf(userId), productIds.indexOf(productId), rating.toDouble) }

// COMMENT: I don't think we need to split create a training and test set, since there is no way of confirming 
// the correctness of our recommendations on the test set.
// val ratingsDF = ratings.toDF()
// val Array(training, test) = ratingsDF.randomSplit(Array(0.8, 0.2))
// val trainingRDD = sc.parallelize(training)

// Build the recommendation model using ALS
val rank = 10
val numIterations = 10
val model = ALS.train(ratings, rank, numIterations, 0.01)

val N = 10;

val recommendations: RDD[(String, Double)]  = productIds.map(baseId =>
    val baseIdInt = productIds.indexOf(baseId)
    val baseItemFactor = model.productFeatures.lookup(baseIdInt).head
    // Get features of the base item as a vector
    val baseItemVector = new DoubleMatrix(baseItemFactor)
    // Find how similar the base item is to every item
    val similarities = model.productFeatures.map{ case (testIdInt, testItemFactor) =>
      // Get features of the test item as a vector
      val testItemVector = new DoubleMatrix(testItemFactor)
      // Compute a similarity score between the two vectors using cosine similarity
      val similarity = cosineSimilarity(baseItemVector, testItemVector)
      // Output a tuple of the test item and its similarity score
      val testId = productIds(testIdInt)
      (testId, similarity)
    }

    // First, the base item itself will be included with top similarity score of 1 so exclude this
    val recs = similarities.filter{ case(testId, similarity) => baseId != testId}
      // Order by descending order of similarity score and get the top N elements
      .top(N)(Ordering.by[(String, Double), Double]) {case (testId, similarity) => similarity}
      // Map each of the top N elements to its itemId
      .map(case(testId, similarity) => testId)
      // How to print output? Want to print (baseId, testId, testId, ...) on same line?
      .forEach(println)

)

// Given the feature vectors of two items, return a double similarity score.
def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
  vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
}


