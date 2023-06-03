// import org.apache.spark.sql.hive.HiveContext

// // 初始化hiveContext
// val hiveCtx = new org.apache.spark.sql.hive.HiveContext(sc)


// 按日期统计用户评分的分布
val mealDataWithData = hiveCtx.sql("select *, (From_Unixtime(ReviewTime, 'yyyy-MM-dd')) AS ReviewData from mealData")
mealDataWithData.registerTempTable("mealDataWithData")


// 创建关键重复记录的DataFrame
val repeatedRatings = hiveCtx.sql("select UserID, MealID, count(*) AS Num from mealData Group by UserID, MealID Having Num > 1 Order by Num desc")


// repeatedRatings: org.apache.spark.sql.DataFrame = [UserID: string, MealID: string, Num: bigint]  
repeatedRatings.registerTempTable("repeatedRatings")

// 联表查询重复记录的特性
// val repeatedRatingsList = hiveCtx.sql("select a.* from mealData a join repeatedRatings b where a.UserID=b.UserID and a.MealID=b.MealID order by a.UserID, a.MealID")
// repeatedRatingsList: org.apache.spark.sql.DataFrame = [MealID: string, Rating: double, Review: string, ReviewTime: bigint, UserID: string]
  