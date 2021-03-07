package scalacode
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{Metadata, StringType, StructField, StructType}
import java.util
import java.util.{ArrayList, HashMap, List, Map, Set, UUID}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.execution.datasources.{BasicWriteTaskStats, ExecutedWriteSummary}
import org.apache.spark.api.java.function.MapFunction
import scala.collection.mutable
import org.apache.spark.sql.Encoders
import java.util.Calendar
import scala.reflect.ClassTag
import org.apache.spark.sql.execution.datasources.WriteTaskResult
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._


//здесь реализуется чтение из таблицы в HBase, парсинг, обработка при помощи Spark и сохранение результата
//в таблицу Hive. Параметры передаются в виде аргументов командной строки 
object HiveWriterWithArgs {

  private val HADOOP_HOME_DIR = "/usr/local/hadoop"

  def createSparkSession(warehousedir:String,metastoreuris:String):SparkSession = {
    val conf = new SparkConf()
    conf.registerKryoClasses(Array
    (classOf[Result],
      Class.forName("scala.math.Ordering$$anon$4"),
      Class.forName("org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering"),
      classOf[Array[org.apache.spark.sql.catalyst.expressions.SortOrder]],
      classOf[org.apache.spark.sql.catalyst.expressions.SortOrder],
      classOf[org.apache.spark.sql.catalyst.expressions.BoundReference],
      Class.forName("org.apache.spark.sql.catalyst.InternalRow$$anonfun$getAccessor$8"),
      classOf[org.apache.spark.sql.catalyst.trees.Origin],
      Class.forName("org.apache.spark.sql.catalyst.expressions.Descending$"),
      Class.forName("org.apache.spark.sql.catalyst.expressions.NullsLast$"),
      classOf[WriteTaskResult],
      classOf[ExecutedWriteSummary],
      classOf[ImmutableBytesWritable],
      classOf[BasicWriteTaskStats],
      Class.forName("org.apache.spark.sql.execution.datasources.WriteTaskResult"),
      Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"),
      Class.forName("org.apache.spark.sql.execution.datasources.ExecutedWriteSummary"),
      Class.forName("org.apache.spark.sql.execution.datasources.BasicWriteTaskStats"),
      classOf[GenericRow],
      classOf[List[Array[String]]],
      classOf[Array[Cell]],
      ClassTag(Class.forName("org.apache.hadoop.hbase.Cell")).runtimeClass,
      Class.forName("org.apache.hadoop.hbase.KeyValue"),
      Class.forName("org.apache.hadoop.hbase.SizeCachedNoTagsKeyValue"),
      Class.forName("org.apache.hadoop.hbase.NoTagsKeyValue"),
      Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"),
      classOf[Row],
      classOf[Array[Row]],
      classOf[GenericRowWithSchema],
      classOf[StructType],
      classOf[StructField],
      classOf[Array[StructField]],
      classOf[StringType],
      classOf[Array[StringType]],
      classOf[List[StringType]],
      classOf[mutable.HashMap[StringType,StringType]],
      classOf[mutable.HashMap[StringType,Array[StringType]]],
      classOf[Metadata],
      Class.forName("org.apache.spark.sql.types.StringType$"),
      Class.forName("org.apache.spark.sql.types.StructField"),
      ClassTag(Class.forName("org.apache.spark.sql.types.StructField$")).runtimeClass,
      Class.forName("org.apache.spark.streaming.dstream.DStream"),
      classOf[org.apache.spark.sql.catalyst.InternalRow],
      classOf[Array[org.apache.spark.sql.catalyst.InternalRow]],
      classOf[org.apache.spark.sql.catalyst.expressions.UnsafeRow],
      classOf[Array[org.apache.spark.sql.catalyst.expressions.UnsafeRow]],
      classOf[Parameters]
    ))

    val spark = SparkSession
      .builder()
      .master("yarn")
      .appName("tohivefromhb")
      .config(conf)
      .config("spark.sql.warehouse.dir", String.format("%s",warehousedir))
      .config("hive.metastore.uris", String.format("%s",metastoreuris))
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrationRequired", "true")
      .enableHiveSupport()
      .getOrCreate()
    spark
  }

  def tmpfunc4(row:Row):Long = {
    var res = 0L
    try{
      res = Bytes.toLong(row.getAs[Array[Byte]]("ts"))
    }
    catch {
      case ex:java.lang.Exception=>
        res = 0L
    }
    return res
  }

  def transformInput1(ts:Long,input:util.List[Array[String]]):Iterator[(String,Array[String])] = {
    var lst = collection.immutable.List[(String,Array[String])]()
    for(i<-0 to input.size()-1){
      val uuid = UUID.randomUUID();
      val tsuuid = ts.toString +"@"+ uuid.toString
      val arr = input.get(i)
      val arr1 = new Array[String](arr.length)
      arr.copyToArray(arr1)
      lst = lst:+(tsuuid,arr1)
    }
    val res = lst.toIterator
    res
  }

  def transformInput2(inp:(String,Array[String])):Row = {
    try {
      val ts = inp._1
      val arr = inp._2
      val tmp = new Array[String](arr.length + 1)
      var isempty = true
      for (i <- 0 to arr.length - 1) {
        if (arr(i) != "") {
          isempty = false
        }
      }
      if (isempty) {
        tmp(0) = "0"
      }
      else {
        tmp(0) = ts
      }
      for (j <- 1 to tmp.length - 1) {
        tmp(j) = arr(j - 1)
      }
      val seq = tmp.toSeq
      val row = Row.fromSeq(seq)
      row
    }
    catch {
      case ex: NullPointerException=>println("NullPointerException")
        null
    }
  }

  def firstnotempty(lst:util.List[Nothing]): String ={
    var res = ""
    val arr = lst.toArray().reverse
    for(i<-0 to arr.length-1){
      if(arr(i).toString!=""){
        return arr(i).toString
      }
    }
    res
  }


  //обработка сразу всего датафрейма, без разделения по значениям поля name
  def createDF(spark:SparkSession,df:DataFrame,parameters:Parameters,
               singlefields:Array[String]) = {
    val fieldIndexMap = parameters.newstruct.fieldNames.zipWithIndex.toMap
    val fieldnames = df.schema.fieldNames
    val oldfieldIndexMap = fieldnames.zipWithIndex.toMap
    var map = collection.immutable.Map[String, String]()
    map = map + ("ts" -> "collect_list")
    for (i <- 0 to fieldnames.length - 1) {
      map = map + (fieldnames(i) -> "collect_list")
    }
    val blank = df.groupBy("LoginName").agg(map).drop("LoginName")
    val lst = blank.schema.fieldNames
    var mapping = mutable.HashMap[Int,Int]()
    for(i<-0 to lst.length-1){
      mapping.put(oldfieldIndexMap(lst(i).replace("collect_list(","").replace(")","")),i)
    }
    val res1 = blank.map(new MapFunction[Row,Row] {
      override def call(inp:Row):Row = {
        val size = parameters.newstruct.size
        val rowaslist = inp.getList _
        val arr = new Array[String](size)
        arr(0) = firstnotempty(rowaslist(mapping(oldfieldIndexMap("ts"))))
        for(i<-0 to singlefields.length-1){
          arr(i + 1) = firstnotempty(rowaslist(mapping(oldfieldIndexMap(singlefields(i)))))
        }
        val keys = parameters.objectkeys.keySet().toArray()
        for(i<-0 to keys.length-1){
          val fieldset = parameters.objectkeys.get(keys(i))
          val mainkey = parameters.mainkeys.get(keys(i))
          val delflag = parameters.delflags.get(keys(i))
          val objects = fillObjectsList(inp,mapping,fieldnames,fieldset,mainkey,delflag,oldfieldIndexMap)
          val result = fillObjectString(objects)
          arr(fieldIndexMap(keys(i).toString)) = result
        }
        val row = Row.fromSeq(arr.toSeq)
        row
      }
    },Encoders.javaSerialization(classOf[Row]))
    val res = spark.createDataFrame(res1.rdd,parameters.newstruct)
    res
  }


  // используется для определения, содержится ли конкретное значение по ключу key в списке deletions
  def contains(value:String,deletions:collection.immutable.List[Array[String]]):Boolean = {
    var flag = false
    if(deletions.size>0) {
      for (i <- 0 to deletions.size - 1) {
        if (deletions(i)(2).equals(value)) {
          flag = true
          deletions(i)(0) = "1"
        }
      }
    }
    flag
  }

  //создание массива массивов String, представляющих собой наборы значений по всем полям в fieldset
  def constrEntryArray(firstrow:Row,
                       fieldset:Array[String],
                       mapping:mutable.HashMap[Int,Int],
                       oldfieldIndexMap:collection.immutable.Map[String,Int]):Array[Array[String]] = {
    val tmp = firstrow.getList _
    val rowcount = tmp(0).size()
    val entryarray = new Array[Array[String]](rowcount)
    for(i<-0 to rowcount-1){
      val elem = new Array[String](fieldset.length)
      for(j<-0 to fieldset.length-1){
        val current = tmp(mapping(oldfieldIndexMap(fieldset(j)))).toArray().reverse
        elem(j) = current(i).toString
      }
      entryarray(i) = elem
    }
    entryarray
  }

  def fillObjectsList(firstrow:Row,
                      mapping:mutable.HashMap[Int,Int],
                      fieldnames:Array[String],
                      fieldset:Array[String],
                      mainkey:String,
                      delflag:String,
                      oldfieldIndexMap:collection.Map[String,Int]):collection.immutable.List[Array[String]]={
    val tmp = firstrow.getList _
    val rowcount = tmp(0).size()
    var objects = collection.immutable.List[Array[String]]()
    var deletions = collection.immutable.List[Array[String]]()
    val fieldIndexMap = fieldset.zipWithIndex.toMap
    for(j<-0 to rowcount-1){
      val entry = new Array[String](fieldnames.length)
      for(i<-0 to fieldset.length-1){
        val current = tmp(mapping(oldfieldIndexMap(fieldnames(i)))).toArray().reverse
        entry(i) = current(j).toString
      }
      val mainvalue = entry(oldfieldIndexMap(mainkey))
      val elem = new Array[String](fieldset.length)
      if(contains(mainvalue,deletions)==false) {
        if(delflag!="" && entry(oldfieldIndexMap(delflag))=="true"){
          val timestamp = entry(oldfieldIndexMap("ts"))
          val deletion = Array("",timestamp,mainvalue)
          deletions = deletions:+deletion
        }
        else {
          for (k <- 0 to fieldset.length - 1) {
            elem(k) = entry(oldfieldIndexMap(fieldset(k)))
          }
          objects = objects :+ elem
        }
      }
    }
    if(deletions.size>0) {

    }
    objects
  }


  def fillObjectString(objects:collection.immutable.List[Array[String]]):String = {
    var res = ""
    for(i<-0 to objects.size-1){
      if(objects(i)(0)!="" && objects(i)(0)!=null){
        for(j<-0 to objects(i).length-1){
          res = res+objects(i)(j)+";"
        }
        res = res+"#\n"
      }
    }
    val arr = res.split("#\n").distinct
    res = arr(0)
    for(i<-1 to arr.length-1){
      res = res+arr(i)+"#\n"
    }
    res
  }

  def writeFromHBtoHive (spark:SparkSession,parameters: Parameters,hbasetable:String,hivetable:String,hbconfig:String):Unit = {
    val df = spark.sqlContext.read.format("org.apache.hadoop.hbase.spark")
      .option("hbase.table",String.format("%s",hbasetable))
      .option("hbase.columns.mapping",
        "rec BINARY id:rec, ts BINARY id:ts")
      .option("hbase.use.hbase.context", false)
      .option("hbase.config.resources", String.format("%s",hbconfig))
      .option("hbase-push.down.column.filter", false)
      .load()

    val currenttime = Calendar.getInstance().getTimeInMillis
    val timeshift = 3628800000L
    val delta = (currenttime - timeshift)
    val timefiltered = df.filter(f=>tmpfunc4(f)>delta)

    val tools = new Tools()
    import spark.implicits._
    var df1 = timefiltered.flatMap(f => transformInput1(
      tmpfunc4(f),
      Tools3.findmultipleComplexKey
      (Bytes.toString(f.getAs[Array[Byte]]("rec")),parameters.entryitempath,parameters.systemprefix,
        parameters.systeminfo,parameters.complexoperidpath,parameters.singles, parameters.objectkeys, parameters.indices)))

    df1 = df1.persist()
    val rdd2 = df1.rdd.map(f => transformInput2(f))
    var df3 = spark.createDataFrame(rdd2, parameters.oldstruct)
    df3 = df3.repartition(6,df3.col("LoginName"))

    val singlefields1 = parameters.systeminfo:+parameters.complexoperid:+parameters.entryname:+parameters.seq
    val singlefields = singlefields1++parameters.singles
    val expr = String.format("DROP TABLE IF EXISTS %s",hivetable)
    spark.sql(expr)
    val resultdf = createDF(spark, df3,parameters,singlefields)
    resultdf.write.mode(SaveMode.Overwrite).saveAsTable(String.format("%s",hivetable))
  }


  def main(args : Array[String]): Unit = {
    val parameters = new Parameters(args(0))
    System.setProperty("hadoop.home.dir", HADOOP_HOME_DIR)
    val hbconf = HBaseConfiguration.create
    val spark = createSparkSession(args(1),args(2))
    var hw = HiveWriterWithArgs
    hw.writeFromHBtoHive(spark,parameters,args(3),args(4),args(5))
  }
}
