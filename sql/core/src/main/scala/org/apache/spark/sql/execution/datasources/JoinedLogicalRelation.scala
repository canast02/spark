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

/*
package org.apache.spark.sql.execution.datasources

import scala.collection.mutable
import org.apache.spark.annotation.{Experimental, InterfaceStability}
import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{Join, LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.sources.{BaseRelation, CanPushJoinsRelation}
import org.apache.spark.util.Utils

/**
 * ::Experimental::
 * An interface for experimenting with pushing joins down to data sources, if possible.
 */
@Experimental
@InterfaceStability.Unstable
class JoinedLogicalRelation(
    override val left: LogicalPlan,
    override val right: LogicalPlan,
    override val joinType: JoinType,
    override val condition: Option[Expression])
  extends Join(left, right, joinType, condition) with CanPushJoinsRelation {

  private[sql] def collectRelationLeaves() = {
    collectLeaves().filter(_.isInstanceOf[LogicalRelation])
        .map(_.asInstanceOf[LogicalRelation])
  }

  override def canJoinScan(relation: BaseRelation): Boolean = {
    collectRelationLeaves()
            .map { _.relation }
            .forall { rel =>
              rel.isInstanceOf[CanPushJoinsRelation] &&
              rel.asInstanceOf[CanPushJoinsRelation].canJoinScan(relation)
            }
  }
}

object JoinedLogicalRelation {
  def apply(left: LogicalPlan, right: LogicalPlan, joinType: JoinType,
            condition: Option[Expression]): JoinedLogicalRelation = {
    new JoinedLogicalRelation(left, right, joinType, condition)
  }

  type ResultType = (LogicalPlan, LogicalPlan, JoinType, Option[Expression])
  def unapply(joinedRelation: JoinedLogicalRelation): Option[ResultType] = Some(
    (joinedRelation.left, joinedRelation.right, joinedRelation.joinType, joinedRelation.condition)
  )
}

private[sql]
case class JoinedRelation(
    expectedOutputAttributes: Option[Seq[Attribute]] = None,
    catalogTable: Option[CatalogTable] = None)
  extends LeafNode with MultiInstanceRelation {

  private val _relations: mutable.MutableList[LogicalRelation] = mutable.MutableList()

  def relations: Seq[BaseRelation] = _relations.map(_.relation)

  private[sql] def addRelation(other: LogicalRelation) = {
    _relations += other
    this
  }

  private[sql] def addRelations(others: TraversableOnce[LogicalRelation]) = {
    _relations ++= others
    this
  }

  override val output: Seq[AttributeReference] = {
    val attrs = _relations.flatMap(_.schema.toAttributes)
    expectedOutputAttributes.map { expectedAttrs =>
      assert(expectedAttrs.length == attrs.length)
      attrs.zip(expectedAttrs).map {
        // We should respect the attribute names provided by base relation and only use the
        // exprId in `expectedOutputAttributes`.
        // The reason is that, some relations(like parquet) will reconcile attribute names to
        // workaround case insensitivity issue.
        case (attr, expected) => attr.withExprId(expected.exprId)
      }
    }.getOrElse(attrs)
  }

  // Joined Relations are distinct if they have different output for the sake of transformations.
  override def equals(other: Any): Boolean = other match {
    case l: JoinedRelation =>
      _relations.forall(rel => l._relations.contains(rel)) && output == l.output
    case _ => false
  }

  override def hashCode: Int = {
    com.google.common.base.Objects.hashCode(_relations, output)
  }

  @transient override def computeStats(conf: CatalystConf): Statistics = {
    catalogTable.flatMap(_.stats.map(_.toPlanStats(output))).getOrElse(
      Statistics(sizeInBytes = relations.map(_.sizeInBytes).sum))
  }

  /**
    * Returns a new instance of this LogicalRelation. According to the semantics of
    * MultiInstanceRelation, this method returns a copy of this object with
    * unique expression ids. We respect the `expectedOutputAttributes` and create
    * new instances of attributes in it.
    */
  override def newInstance(): this.type = {
    JoinedRelation(
      expectedOutputAttributes.map(_.map(_.newInstance())),
      catalogTable).addRelations(_relations).asInstanceOf[this.type]
  }

  override def simpleString: String = {
    val joinRels = _relations.map(_.relation.toString).mkString(" JOIN ")
    s"JoinedRelation[${Utils.truncatedString(output, ",")}] $joinRels"
  }
}
*/