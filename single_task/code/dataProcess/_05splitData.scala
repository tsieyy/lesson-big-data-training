// 数据分割

//加载数据
val inputPath = "/opt/share/single_task/data/RatingCodeList"
val RatingCodeList = sc.textFile(inputPath, 6).map{x => val fields=x.slice(1, x.size-1).split(",");(fields(0).toInt, fields(1).toInt, fields(2).toDouble, fields(3).toLong)}.sortBy(_._4)
val zipRatingCodeList = RatingCodeList.zipWithIndex.mapValues(x=>(x+1))
// 定义数据分割点
val totalNum = RatingCodeList.count()
val splitPoint1 = totalNum * 0.8 toInt
val splitPoint2 = totalNum * 0.9 toInt
// 生成训练集数据
val train = zipRatingCodeList.filter(x=>(x._2<splitPoint1)).map(x=>(x._1._1, x._1._2, x._1._3))
// 生成验证集数据
val validate = zipRatingCodeList.filter(x=>(x._2>=splitPoint1 && x._2<splitPoint2)).map(x=>(x._1._1, x._1._2, x._1._3))
val test = zipRatingCodeList.filter(x=>(x._2>=splitPoint2)).map(x=>(x._1._1, x._1._2, x._1._3))

train.repartition(6).saveAsTextFile("/opt/share/single_task/data/trainRatings")

validate.repartition(6).saveAsTextFile("/opt/share/single_task/data/validateRatings")

test.repartition(6).saveAsTextFile("/opt/share/single_task/data/testRatings")
