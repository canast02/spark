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

package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, PushDownJoin}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.sources._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._

class DefaultSource extends SchemaRelationProvider with DataSourceRegister {
  /**
    * The string that represents the format that this data source provider uses. This is
    * overridden by children to provide a nice alias for the data source. For example:
    *
    * {{{
    *   override def shortName(): String = "parquet"
    * }}}
    *
    * @since 1.5.0
    */
  override def shortName(): String = "test_source"

  /**
    * Returns a new base relation with the given parameters.
    *
    * @note The parameters' keywords are case insensitive and this insensitivity is enforced
    *       by the Map that is passed to the function.
    */
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String],
                              schema: StructType): BaseRelation = {
    new DummyRelation(sqlContext, schema, parameters)
  }
}


class DummyRelation(val sqlContext: SQLContext, val schema: StructType,
                    parameters: Map[String, String])
    extends BaseRelation with TableScan with PrunedFilteredScan
        with JoinedScan with Serializable {

  private def __initialize(): IndexedSeq[Seq[Any]] = {
    def generateRow(rand: scala.util.Random): Seq[Any] =
    schema.map(f => f.dataType match {
      case IntegerType => rand.nextInt(300)
      case DoubleType => rand.nextDouble() * 300
      case StringType => rand.nextInt(300).toString
    })

    val rand = scala.util.Random
    rand.setSeed(1000L)
    (0 to 50).map(_ => generateRow(rand))
  }

  private val _params = parameters
  private var _data: IndexedSeq[Seq[Any]] = __initialize()

  def datasourceName(): String = "S1"

  override def buildScan(): RDD[Row] = {
    sqlContext.sparkContext.parallelize(_data).map { row =>
      Row.fromSeq(row)
    }
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    sqlContext.sparkContext.parallelize(_data).map { row =>
      val typed = row.zip(schema).map {
        case (v, field) if requiredColumns.contains(field.name) => v
      }
      Row.fromSeq(typed)
    }
  }

  override def buildScan(requiredColumns: Seq[Attribute], filters: Seq[Expression]): RDD[Row] = {
    var df = sqlContext
        .createDataFrame(buildScan(), schema)

    for ( exp <- filters ) {
      df = df.filter(exp.sql).toDF()
    }

    df.select(requiredColumns.map(a => Column(a.sql)) : _*).rdd
  }

  override def canJoinScan(relation: BaseRelation): Boolean = {
    relation.isInstanceOf[DummyRelation] &&
        relation.asInstanceOf[DummyRelation].datasourceName() == datasourceName()
  }

  override def createJoinedRelation(other: JoinedScan): BaseRelation = {
    val left = sqlContext.createDataFrame(buildScan(), schema)
    val right = sqlContext.createDataFrame(other.asInstanceOf[TableScan].buildScan(),
      other.asInstanceOf[BaseRelation].schema)

    val joined = left.crossJoin(right)
    val newSchema = joined.schema

    val newName = _params.getOrElse("name", "") + "_JOIN_" +
        other.asInstanceOf[DummyRelation]._params.getOrElse("name", "")
    val newParams = Map[String, String]("name" -> newName)

    val ret = new DummyRelation(sqlContext, newSchema, newParams)
    ret._data = joined.rdd.collect().map(_.toSeq).toIndexedSeq

    ret
  }

  override def toString: String = s"DummyRelation (${parameters.getOrElse("name", "")})"
}

class JoinPushSuite extends QueryTest with SharedSQLContext {

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.read.format("jdbc")
        .options(Map(
          "url" -> "jdbc:mysql://localhost:3306/employees?serverTimezone=UTC&useSSL=false",
          "user" -> "demouser",
          "password" -> "demo.pass1234",
          "driver" -> "com.mysql.jdbc.Driver",
          "dbtable" -> "employees"
        ))
        .option("joinable", "true")
        .load()
        .createOrReplaceTempView("employees")

    spark.read.format("jdbc")
        .options(Map(
          "url" -> "jdbc:mysql://localhost:3306/employees?serverTimezone=UTC&useSSL=false",
          "user" -> "demouser",
          "password" -> "demo.pass1234",
          "driver" -> "com.mysql.jdbc.Driver",
          "dbtable" -> "departments"
        ))
        .option("joinable", "true")
        .load()
        .createOrReplaceTempView("departments")

    spark.read.format("jdbc")
        .options(Map(
          "url" -> "jdbc:mysql://localhost:3306/employees?serverTimezone=UTC&useSSL=false",
          "user" -> "demouser",
          "password" -> "demo.pass1234",
          "driver" -> "com.mysql.jdbc.Driver",
          "dbtable" -> "current_dept_emp"
        ))
        .option("joinable", "true")
        .load()
        .createOrReplaceTempView("dept_emp")

    val builder = new MetadataBuilder()
    builder.putLong("q", 1)
    var m = builder.build()
    spark.read.format("org.apache.spark.sql.execution")
        .schema(StructType(
          StructField("a", IntegerType, nullable = false, m) ::
              StructField("b", IntegerType, nullable = false, m) ::
              StructField("c", IntegerType, nullable = false, m) ::
              Nil
        ))
        .option("name", "tbl1")
        .load()
        .createOrReplaceTempView("tbl1")

    builder.putLong("q", 2)
    m = builder.build()
    spark.read.format("org.apache.spark.sql.execution")
        .schema(StructType(
          StructField("e", IntegerType, nullable = false, m) ::
              StructField("f", IntegerType, nullable = false, m) ::
              Nil
        ))
        .option("name", "tbl2")
        .load()
        .createOrReplaceTempView("tbl2")

    builder.putLong("q", 3)
    m = builder.build()
    spark.read.format("org.apache.spark.sql.execution")
        .schema(StructType(
          StructField("g", IntegerType, nullable = false, m) ::
              StructField("h", IntegerType, nullable = false, m) ::
              StructField("i", IntegerType, nullable = false, m) ::
              Nil
        ))
        .option("name", "tbl3")
        .load()
        .createOrReplaceTempView("tbl3")
  }

  override protected def afterAll(): Unit = {
    try {
      spark.sqlContext.dropTempTable("employees")
      spark.sqlContext.dropTempTable("departments")
      spark.sqlContext.dropTempTable("dept_emp")

      spark.sqlContext.dropTempTable("tbl1")
      spark.sqlContext.dropTempTable("tbl2")
      spark.sqlContext.dropTempTable("tbl3")
    } finally {
      super.afterAll()
    }
  }

  // scalastyle:off println
  def timeit[R](block: => R, timeunit: String = "ms"): R = {
    val start = System.nanoTime()
    val result = block
    val end = System.nanoTime()
    timeunit match {
      case "nano" => println(s"Elapsed time: ${end - start}ns")
      case "ms" => println(s"Elapsed time: ${(end - start) / 1000000}ms")
      case "sec" => println(s"Elapsed time: ${(end - start) / 1000000000}s")
      case _ => println(s"Elapsed time: ${(end - start) / 1000000}ms")
    }
    if (!result.isInstanceOf[Unit]) {
      println(s"Result: $result")
    }
    result
  }

  def withOptimization(rule: Rule[LogicalPlan])(fn: => Unit): Unit = {
    spark.sessionState.experimentalMethods.extraOptimizations = rule :: Nil
    fn
    spark.sessionState.experimentalMethods.extraOptimizations = Nil
  }

  val query1: String = "select * from employees e, dept_emp de, departments d " +
      "where e.emp_no = de.emp_no and de.dept_no = d.dept_no"
  val query2: String = "select e.emp_no, e.first_name, e.last_name, d.dept_no, d.dept_name " +
      "from employees e, dept_emp de, departments d " +
      "where e.emp_no = de.emp_no and de.dept_no = d.dept_no"

//  val dummy_q: String = "select * from tbl1, tbl2, tbl3 " +
//      "where tbl1.a = tbl2.e and tbl2.f = tbl3.i and tbl1.b > 15"
//  test("Dummy Relation test") {
//    val df = sql(dummy_q)
//    val optPlan = df.queryExecution.optimizedPlan
//    println(optPlan)
//    assert(optPlan != null)
//    val physicalPlan = df.queryExecution.executedPlan
//    println(physicalPlan)
//    assert(physicalPlan != null)
//    df.show()
//  }
//
//  test("Dummy Relation with Join PushDown test") {
//    withOptimization(PushDownJoin) {
//      val df = sql(dummy_q)
//      val qExec = df.queryExecution
//      val optPlan = qExec.optimizedPlan
//      println(optPlan)
//      assert(optPlan != null)
//      val physicalPlan = qExec.executedPlan
//      println(physicalPlan)
//      assert(physicalPlan != null)
//      df.show()
//    }
//  }
//
//  test("Dummy Relation Join Results Equal") {
//    val df1 = sql(dummy_q).sort("a").collect()
//    withOptimization(PushDownJoin) {
//      val df2 = sql(dummy_q).sort("a").collect()
//      assert(df1.sameElements(df2))
//    }
//  }

  test("Without Pushing Joins to JDBC source") {
    val qExec = sql(query1).queryExecution
    val logicalPlan = qExec.optimizedPlan
    println(logicalPlan)
    assert(logicalPlan != null)
    val physicalPlan = qExec.executedPlan
    println(physicalPlan)
    assert(physicalPlan != null)
  }

  test("With Pushing Joins to JDBC source") {
    withOptimization(PushDownJoin) {
      val qExec = sql(query1).queryExecution
      val logicalPlan = qExec.optimizedPlan
      println(logicalPlan)
      assert(logicalPlan != null)
      val physicalPlan = qExec.executedPlan
      println(physicalPlan)
      assert(physicalPlan != null)
    }
  }

  test("With Selection and Pushing Joins to JDBC source") {
    withOptimization(PushDownJoin) {
      val logicalPlan = sql(query2).queryExecution.optimizedPlan
      println(logicalPlan)
      assert(logicalPlan != null)
    }
  }

}
