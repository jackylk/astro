/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hbase.examples

import org.apache.spark.sql.hbase.HBaseSQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * This example shows how to create DF,load data and query data from DataFrame.
 */

case class People(name: String, age: Int, id: Int, address: String)

object AstroDataFrameExample {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().
      setAppName("AstroTest").
      setMaster("local").
      set("spark.hadoop.hbase.zookeeper.quorum", "localhost")
    //creating HBaseSQLContext.
    val sc = new SparkContext(sparkConf)
    val hsc = new HBaseSQLContext(sc)

    import hsc.implicits._

    //Creating people table
    hsc.read.format("org.apache.spark.sql.hbase.HBaseSource").options(
      Map("namespace" -> "",
        "tableName" -> "people",
        "hbaseTableName" -> "people_table",
        "colsSeq" -> "name,age,id,address",
        "keyCols" -> "id,integer",
        "nonKeyCols" -> "name,string,cf1,cq_name;age,integer,cf1,cq_age;address,string,cf2,cq_address",
        "encodingFormat" -> "binaryformat")).load

    // Load the data from text file and create DF
    val peopleDF = sc.textFile("examples/src/main/resources/people.txt").map(_.split(",")).
        map(p=>People(p(0), p(1).toInt, p(2).toInt, p(3))).toDF()
    //Insert peopleDF data to people table
    peopleDF.insertInto("people")

    //Create DF from people table
    val df = hsc.table("people")
    df.printSchema()
    //Query data from people table
    df.select(df("name"), df("age") + 1).show()
  }

}
