package com.atguigu.gmall1122.realtime.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import scala.collection.mutable

object OffsetManager {

  //把redis中的偏移量读取出来，并转换成kafka需要的偏移量格式
  def getOffset(groupId: String, topic: String): Map[TopicPartition, Long] = {
    val jedis: Jedis = RedisUtil.getJedisClient
    //Redis type? hash  key? offset:[groupid]:[topic] field ? partition value?
    val offsetKey="offset:"+groupId+":"+topic
    val redisOffsetMap: util.Map[String, String] = jedis.hgetAll(offsetKey)
    jedis.close()
    //读出来的是java的map，不能进行转换操作，所以需要导包
    import  scala.collection.JavaConversions._
    val kafkaOffsetMap: mutable.Map[TopicPartition, Long] = redisOffsetMap.map{case (partitionId,offsetStr)=>(new TopicPartition(topic,partitionId.toInt),offsetStr.toLong)}
    kafkaOffsetMap.toMap
  }

  //写入偏移量

  def saveOffset(groupId:String,topic:String,offsetRanges:Array[OffsetRange]) = {
    val jedis: Jedis = RedisUtil.getJedisClient
    val offsetKey="offset:"+groupId+":"+topic
    val offsetMap = new util.HashMap[String,String]()
    for (offsetRange <- offsetRanges){
      offsetMap.put(offsetRange.partition.toString,offsetRange.untilOffset.toString)
    }
    //把各个分区的偏移量写入redis
    jedis.hmset(offsetKey,offsetMap)

    jedis.close()
  }

}
