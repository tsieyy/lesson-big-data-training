// 清洗数据，删除重复的评分数据

// 获得用户与菜品的最新评分日期的组合 （ User1D,Mea11D, LastestDate ）
val latestRatingPair = hiveCtx.sql("select UserID, MealID,MAX(ReviewTime) AS LastestDate from mealData Group by UserID, MealID")
latestRatingPair.registerTempTable("latestRatingPair")

// 联表查询获得各用户最新的评分记录

val lastestRatings = hiveCtx.sql("select a.UserID, a.MealID, a.Rating, a.ReviewTime from mealData a join latestRatingPair b where a.UserID=b.UserID and a.MealID=b.MealID and a.ReviewTime = b.LastestDate")
// 去重后的记录数
lastestRatings.count
// 保存去重后的用户评分记录
import org.apache.spark.sql.SaveMode
val outputPath="/opt/share/single_task/data/cleanMealRatings"
lastestRatings.write.format("json").save(outputPath)


// print("Exception handing complete!\n") 

