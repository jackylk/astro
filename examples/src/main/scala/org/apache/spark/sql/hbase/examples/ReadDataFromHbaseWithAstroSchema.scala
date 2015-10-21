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
import org.apache.hadoop.hbase.client.{Result, Scan, HTable}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.hbase.KeyColumn
import org.apache.spark.sql.hbase.util.{BinaryBytesUtils, HBaseKVHelper}
import org.apache.spark.sql.types.{IntegerType, StringType, DoubleType}

/**
 * This example explains how to write data to Hbase table for Astro backend schema.
 * For example the following table is created in Astro with following schema.
 *   CREATE TABLE teacher(grade int, class int, subject string, teacher_name string,
 *   teacher_age int, PRIMARY KEY (grade, class, subject))
 *   MAPPED BY (hbase_teacher, COLS=[teacher_name=teachercf.name, teacher_age=teachercf.age]);
 *  Astro uses its own encode and decode of keys, so we should Astro APIs to convert them
 */
object ReadDataFromHbaseWithAstroSchema {
  def main(args: Array[String]): Unit = {
    val config = HBaseConfiguration.create
    val table = new HTable(config, "hbase_teacher")
    val scan = new Scan();
    // Scanning the required columns
    scan.addColumn(Bytes.toBytes("teachercf"), Bytes.toBytes("name"));
    scan.addColumn(Bytes.toBytes("teachercf"), Bytes.toBytes("age"))
    // Getting the scan result
    val scanner = table.getScanner(scan);
    // Reading values from scan result
    var result = scanner.next()
    while (result != null) {
      //Get the row key
      val rowKey = result.getRow
      //Decode the row keys.The columns which are specified in PRIMARY KEY
      val keys = HBaseKVHelper.decodingRawKeyColumns(rowKey,
        Seq(KeyColumn("grade", IntegerType, 0),
          KeyColumn("class", IntegerType, 1),
          KeyColumn("subject", StringType, 2)))
      val grade = BinaryBytesUtils.toInt(rowKey, keys(0)._1)
      val _class = BinaryBytesUtils.toInt(rowKey, keys(1)._1)
      val subject = BinaryBytesUtils.toUTF8String(rowKey, keys(2)._1, keys(2)._2)

      //Decode column keys
      val nameKey = result.getValue(Bytes.toBytes("teachercf"), Bytes.toBytes("name"))
      val name = BinaryBytesUtils.toUTF8String(nameKey, 0, nameKey.length)
      val age = BinaryBytesUtils.toInt(result.getValue(Bytes.toBytes("teachercf"), Bytes.toBytes("age")), 0)

      //print the results
      println("grade: "+grade+"|class: "+_class+"|subject: "+subject +"|name: "+name+"|age: "+age)
      result = scanner.next()
    }

  }
}
