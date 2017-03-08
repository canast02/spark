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

package isi.edu.connectors

// scalastyle:off
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object Testing {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    val sparkSession = SparkSession.builder().appName("Push Down").master("local[2]").getOrCreate()
    val sqlctx = sparkSession.sqlContext

    val df = sqlctx.read.format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://dokdo.usc.edu:3306/employees?serverTimezone=UTC&useSSL=false")
      .option("user", "stripelis")
      .option("password", "stripelis13")
      .option("dbtable", "departments")
      .load()

    df.createOrReplaceTempView("departments")
    val res = sqlctx.sql("select * from departments")
    res.show()
    println(res.count())

  }

}
