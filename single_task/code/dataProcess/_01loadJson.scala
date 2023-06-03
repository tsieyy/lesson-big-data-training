import org.apache.spark.sql.hive.HiveContext

val hiveCtx = new org.apache.spark.sql.hive.HiveContext(sc)

val inputPath = "../../data/MealRatings_201705_201706.json"

val mealData = hiveCtx.jsonFile(inputPath)

mealData.registerTempTable("mealData")

val mealResults = hiveCtx.sql("select UserID, MealID, Rating, Review, ReviewTime from mealData")

mealResults.take(5).toList.foreach(println)