package com.atguigu.gmall1122.realtime.util

import java.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index, Search}

object MyEsUtil {
  private    var  factory:  JestClientFactory=null;

  def getClient:JestClient ={
    if(factory==null)build();
    factory.getObject

  }

  def  build(): Unit ={
    factory=new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder("http://hadoop105:9200" )
      .multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(10000).readTimeout(1000).build())

  }

  //batch
  def saveBulk(dataList:List[(String,AnyRef)],indexName:String):Unit = {
    if (dataList != null&&dataList.size>0){
      val jest: JestClient = getClient
      val bulkBuilder = new Bulk.Builder
      bulkBuilder.defaultIndex(indexName).defaultType("_doc")
      for((id,data) <- dataList) {
        val index: Index = new Index.Builder(data).id(id).build()
        bulkBuilder.addAction(index)
      }
      val bulk: Bulk = bulkBuilder.build()
      val items: util.List[BulkResult#BulkResultItem] = jest.execute(bulk).getItems
      println("已保存："+items.size()+"条数据!")
      jest.close()
    }
  }

  def main(args: Array[String]): Unit = {
    val jest: JestClient = getClient
    //val index: Index = new Index.Builder(Movie(4,"红海战役",9.0)).index("movie_chn1122").`type`("movie").id("4").build()
    //new Search.Builder().addIndex()
    //jest.execute(index)
    jest.close()
  }


  case class Movie(i: Int, str: String, d: Double)

}
