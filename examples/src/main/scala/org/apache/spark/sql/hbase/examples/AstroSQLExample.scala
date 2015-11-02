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
 * Example to create HbaseSQLContext and then create table,insert table and query from table.
 */
object AstroSQLExample {

  def main (args: Array[String]){
    val sparkConf = new SparkConf().
      setAppName("AstroTest").
      setMaster("local").
      set("spark.hadoop.hbase.zookeeper.quorum", "localhost")
    //creating HBaseSQLContext.
    val sc = new SparkContext(sparkConf)
    val hsc = new HBaseSQLContext(sc)

    //create table
    hsc.sql("CREATE TABLE test_teacher(grade int, class int, subject string, teacher_name string, " +
      "teacher_age int, PRIMARY KEY (grade, class, subject)) " +
      "MAPPED BY (hbase_test_teacher, COLS=[teacher_name=teachercf.name, teacher_age=teachercf.age])")

    //insert into table
    hsc.sql("insert into table test_teacher values(\"1\",\"1\",\"Science\",\"Jessica\",\"30\")")

    // select from table.
    hsc.sql("select * from test_teacher").collect().foreach(println)
  }

}
