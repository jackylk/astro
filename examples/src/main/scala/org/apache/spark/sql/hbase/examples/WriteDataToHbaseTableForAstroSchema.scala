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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.hbase.util.{BinaryBytesUtils, HBaseKVHelper}
import org.apache.spark.sql.types._

/**
 * This example explains how to write data to Hbase table for Astro backend schema.
 * For example the following table is created in Astro with following schema.
 *   CREATE TABLE teacher(grade int, class int, subject string, teacher_name string,
 *   teacher_age int, PRIMARY KEY (grade, class, subject))
 *   MAPPED BY (hbase_teacher, COLS=[teacher_name=teachercf.name, teacher_age=teachercf.age]);
 * Astro uses its own encode and decode of keys, so we should Astro APIs to convert them
 */
object WriteDataToHbaseTableForAstroSchema {
  def main(args: Array[String]): Unit = {
    val config = HBaseConfiguration.create
    val table = new HTable(config, "hbase_teacher")
    val grade = 1
    val _class = 12
    val subject = "English"
    val name = "Tom"
    val age: Int = 30
    //Create the row key.It is the combination of grade, class & subject. These are specified inside PRIMARY KEY
    // of schema
    val rowKey = HBaseKVHelper.encodingRawKeyColumns(
      Seq((BinaryBytesUtils.create(IntegerType).toBytes(grade), IntegerType)
        , (BinaryBytesUtils.create(IntegerType).toBytes(_class), IntegerType)
        , (BinaryBytesUtils.create(StringType).toBytes(subject), StringType)))

    val put: Put = new Put(rowKey)
    //Add all remaining columns as hbase columns apart from  PRIMARY KEY
    put.add(Bytes.toBytes("teachercf"), Bytes.toBytes("name"), BinaryBytesUtils.create(StringType).toBytes(name))
    put.add(Bytes.toBytes("teachercf"), Bytes.toBytes("age"), BinaryBytesUtils.create(IntegerType).toBytes(age))
    table.put(put)
    table.close
  }
}
