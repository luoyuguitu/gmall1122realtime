package com.atguigu.gmall1122.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall1122.realtime.bean.DauInfo
import com.atguigu.gmall1122.realtime.util.{MyEsUtil, MyKafkaUtil, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer


object DauApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("dau_app")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "GMALL_START"
    val groupId = "GMALL_DAU_CONSUMER"
    //判断是否是第一次读取并读取偏移量
    //从redis读取偏移量
    val startOffset: Map[TopicPartition, Long] = OffsetManager.getOffset(groupId,topic)

    var startInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    //如果是从redis读出来了，则用redis的，否则用kafka最新的偏移量
    if (startOffset == null) {
      startInputDstream=MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    } else {
      startInputDstream=MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
    }

    //获取批次的结束偏移量，在最后写到redis中
    var startUpOffsetRanges:Array[OffsetRange] = null
    val startupInputGetoffsetDstream: DStream[ConsumerRecord[String, String]] = startInputDstream.transform { rdd =>
      startUpOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }



    //startInputStream.map(_.value).print(1000)

    val startJsonObjDstream: DStream[JSONObject] = startupInputGetoffsetDstream.map { record =>
      val jsonString: String = record.value()
      val jSONObject: JSONObject = JSON.parseObject(jsonString)
      jSONObject
    }
    //redis写入过滤清单，以分区为单位，节省资源，并过滤数据，利用返回值是否为1
    /*startJsonObjDstream.map{jsonObj=>
      val dateStr: String = new SimpleDateFormat("yyyyMMdd").format(jsonObj.getLong("ts"))
      val dauKey="dau:"+dateStr
      val jedis = new Jedis("hadoop105",6379)
      val mid: String = jsonObj.getJSONObject("common").getString("mid")
      jedis.sadd(dauKey,mid)
      jedis.close()

    }*/
    val startJsonObjWithDauDstream: DStream[JSONObject] = startJsonObjDstream.mapPartitions { jsonObjItr =>
      //val jedis = new Jedis("hadoop105",6379)
      val jedis: Jedis = RedisUtil.getJedisClient
      val jsonObjList: List[JSONObject] = jsonObjItr.toList
      //println("过滤前："+jsonObjList.size)
      val jsonFilteredList = new ListBuffer[JSONObject]()
      for (jsonObj <- jsonObjList) {
        val dateStr: String = new SimpleDateFormat("yyyyMMdd").format(jsonObj.getLong("ts"))
        val dauKey = "dau:" + dateStr

        val mid: String = jsonObj.getJSONObject("common").getString("mid")
        val isFirstFlag: lang.Long = jedis.sadd(dauKey, mid)
        if (isFirstFlag == 1L) {
          jsonFilteredList += jsonObj
        }
      }

      jedis.close()
      //println("过滤后："+jsonFilteredList.size)
      jsonFilteredList.toIterator
    }
    //startJsonObjWithDauDstream.print(1000)
    //要插入gmall1122_dau_info_2020xxxxx
    //转换结构
    val dauInfoDstream: DStream[DauInfo] = startJsonObjWithDauDstream.map { jsonObj =>
      val commonJsonObj: JSONObject = jsonObj.getJSONObject("common")
      val dateTimeStr: String = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(jsonObj.getLong("ts"))
      val dateTimeArr: Array[String] = dateTimeStr.split(" ")
      val dt: String = dateTimeArr(0)
      val timeArr: Array[String] = dateTimeArr(1).split(":")
      val hr = timeArr(0)
      val mi = timeArr(1)
      DauInfo(commonJsonObj.getString("mid"),
        commonJsonObj.getString("uid"),
        commonJsonObj.getString("ar"),
        commonJsonObj.getString("ch"),
        commonJsonObj.getString("vc"),
        dt, hr, mi, jsonObj.getLong("ts")
      )
    }
    //插入gmall1122_dau_info_2020xxxxx
    dauInfoDstream.foreachRDD { rdd =>
      rdd.foreachPartition { dauInfoItr =>
        //观察偏移量移动
        val offsetRange: OffsetRange = startUpOffsetRanges(TaskContext.getPartitionId())
        println("偏移量从："+offsetRange.fromOffset+"-------------->"+offsetRange.untilOffset)
        val dataList: List[(String, DauInfo)] = dauInfoItr.toList.map { dauInfo =>
          (dauInfo.mid, dauInfo)
        }
        val dt = new SimpleDateFormat("yyyyMMdd").format(new Date())
        val indexName = "gmall1122_dau_info_" + dt
        MyEsUtil.saveBulk(dataList, indexName)

        //业务完成，偏移量提交
        OffsetManager.saveOffset(groupId,topic,startUpOffsetRanges)

      }
    }


    ssc.start()
    ssc.awaitTermination()
  }

}
