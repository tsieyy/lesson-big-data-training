import org.apache.spark.sql.hive.HiveContext

// 初始化hiveContext
val hiveCtx = new org.apache.spark.sql.hive.HiveContext(sc)

//读取清洗后的数据，加载到DataFrame
val inputPath = "/opt/share/single_task/data/cleanMealRatings"

val cleanMealData = hiveCtx.jsonFile(inputPath)

// 选取属性（userID，MealID，Rating, ReviewTime）, 生成用户评分数据集
val MealRatings = cleanMealData.select("UserID", "MealID", "Rating", "ReviewTime")
val RatingRDD = MealRatings.map(row => (row.getString(0), row.getString(1), row.getDouble(2), row.getLong(3)))
// 构造用户、菜品的编码集
val userZipCode = RatingRDD.map(_._1).sort("UserID").zipWithIndex.map(data=>(data._1, data._2.toInt))
val mealZipCode = RatingRDD.map(_._2).sort("MealID").zipWithIndex.map(data=>(data._1, data._2.toInt))
val userZipCodeMap = userZipCode.collect.toMap
val mealZipCodeMap = mealZipCode.collect.toMap

//以用户菜品编码，重新构造评分数据集
val RatingCodeList = RatingRDD.map(x=>(userZipCodeMap(x._1), mealZipCode(x._2), x._3, x._4)).sort(x=>x._4)
RatingCodeList.take(5)

//存储用户、菜品的编码集和评分数据集
userZipCode.repartition(1).saveAsTextFile("/opt/share/single_task/data/userZipCode")
mealZipCode.repartition(1).saveAsTextFile("/opt/share/single_task/data/mealZipCode")

