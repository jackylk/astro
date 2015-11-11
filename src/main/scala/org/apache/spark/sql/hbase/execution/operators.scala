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

package org.apache.spark.sql.hbase.execution

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Row, execution}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.Subquery
import org.apache.spark.sql.hbase.{HBaseSQLContext, HBaseRelation}
import org.apache.spark.sql.hbase.util.DataTypeUtils
import org.apache.spark.unsafe.types.UTF8String

@DeveloperApi
case class UpdateTable(
    tableName: String,
    columnsToUpdate: Seq[Expression],
    values: Seq[String],
    child: SparkPlan) extends execution.UnaryNode {

  override def output: Seq[Attribute] = Seq.empty

  protected override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {
    val solvedRelation =
      sqlContext.asInstanceOf[HBaseSQLContext].catalog.lookupRelation(Seq(tableName))
    val relation: HBaseRelation = solvedRelation.asInstanceOf[Subquery]
      .child.asInstanceOf[LogicalRelation]
      .relation.asInstanceOf[HBaseRelation]

    val typesValues = values.zip(columnsToUpdate.map(_.dataType)).map { v =>
      val typeValue = DataTypeUtils.string2TypeData(v._1, v._2)
      if (v._2 == StringType) {
        UTF8String.fromString(v._1) // InternalRow needs UTF8String, so convert it.
      } else {
        typeValue
      }
    }
    val input = child.output
    val mutableRow = new SpecificMutableRow(input.map(_.dataType))
    val ordinals = columnsToUpdate.map { att =>
      BindReferences.bindReference(att, input)
    }.map(_.asInstanceOf[BoundReference].ordinal)

    val rowDataType = input.map(_.dataType)
    val converter = CatalystTypeConverters.createToScalaConverter(StructType.fromAttributes(input))

    val resRdd = child.execute().mapPartitions { iter =>
      val len = input.length
      iter.map { row =>
        var i = 0
        while (i < len) {
          val colValue = rowDataType(i) match {
            case StringType => row.getUTF8String(i)
            case _ => row.get(i, rowDataType(i))
          }
          mutableRow.update(i, colValue)
          i += 1
        }
        ordinals.zip(typesValues).map { x => mutableRow.update(x._1, x._2) }
        converter(mutableRow).asInstanceOf[Row]
      }
    }
    val inputValuesDF = sqlContext.createDataFrame(resRdd, relation.schema)
    relation.insert(inputValuesDF, false)
    sqlContext.sparkContext.emptyRDD[InternalRow]
  }
}

@DeveloperApi
case class DeleteFromTable(tableName: String, child: SparkPlan) extends execution.UnaryNode {
  override def output: Seq[Attribute] = Seq.empty

  protected override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {
    val solvedRelation =
      sqlContext.asInstanceOf[HBaseSQLContext].catalog.lookupRelation(Seq(tableName))

    val relation: HBaseRelation = solvedRelation.asInstanceOf[Subquery]
      .child.asInstanceOf[LogicalRelation]
      .relation.asInstanceOf[HBaseRelation]

    val input = child.output
    val inputValuesDF = sqlContext.internalCreateDataFrame(child.execute(), relation.schema)
    relation.delete(inputValuesDF)
    sqlContext.sparkContext.emptyRDD[InternalRow]
  }
}
