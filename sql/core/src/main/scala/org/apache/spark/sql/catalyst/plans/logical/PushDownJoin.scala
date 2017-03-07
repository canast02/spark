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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.JoinedScan

// scalastyle:off println
object PushDownJoin extends Rule[LogicalPlan] {

  private def areJoinable(l: LogicalPlan, r: LogicalPlan): Boolean = {
    (l, r) match {
      case (_ @ LogicalRelation(a: JoinedScan, _, _),
          _ @ LogicalRelation(b: JoinedScan, _, _)) =>
        a.canJoinScan(b) && b.canJoinScan(a)
      case _ => false
    }
  }

  private def joinRelations(l: LogicalRelation, r: LogicalRelation,
                            joinType: JoinType, cond: Option[Expression]): LogicalPlan = {
    val rel = l.relation
        .asInstanceOf[JoinedScan]
        .createJoinedRelation(r.relation.asInstanceOf[JoinedScan])

    if ( cond.isEmpty ) {
      LogicalRelation(rel,
        Some(l.output ++ r.output),
        l.catalogTable.orElse(r.catalogTable)
      )
    } else {
      Filter(
        cond.get,
        LogicalRelation(rel,
          Some(l.output ++ r.output),
          l.catalogTable.orElse(r.catalogTable)
        )
      )
    }
  }

//  implicit private def filter2collection(f: Filter): FilterCollection = {
//    FilterCollection(f.condition :: Nil, f.child)
//  }

  private def applyIfJoinable(j: Join): LogicalPlan = j match {
    // Merge Logical Relations
    case Join(l: LogicalRelation, r: LogicalRelation, Inner, cond)
    if areJoinable(l, r) =>
      joinRelations(l, r, Inner, cond)

    // Merge even if one side is Filtered
    case Join(l: LogicalRelation, r @ Filter(_, right: LogicalRelation), Inner, cond)
      if areJoinable(l, right) =>
      FilterCollection(r.condition :: Nil, joinRelations(l, right, Inner, cond))
    case Join(l @ Filter(_, left: LogicalRelation), r: LogicalRelation, Inner, cond)
      if areJoinable(left, r) =>
      FilterCollection(l.condition :: Nil, joinRelations(left, r, Inner, cond))

    // Merge even if one side is Filtered
    case Join(l: LogicalRelation, r @ FilterCollection(_, right: LogicalRelation), Inner, cond)
      if areJoinable(l, right) =>
      FilterCollection(r.conditions, joinRelations(l, right, Inner, cond))
    case Join(l @ FilterCollection(_, left: LogicalRelation), r: LogicalRelation, Inner, cond)
      if areJoinable(left, r) =>
      FilterCollection(l.conditions, joinRelations(left, r, Inner, cond))

    case _ => j
  }

//  private def applyIfJoinable(j: Join, l: LogicalRelation, r: LogicalRelation): LogicalPlan = {
//    (l.relation, r.relation) match {
//      case (left: CanPushJoinsRelation, right: CanPushJoinsRelation)
//        if left.canJoinScan(right) && right.canJoinScan(left) =>
//        JoinedLogicalRelation(l, r, j.joinType, j.condition)
//      case _ => j
//    }
//  }
//
//  private def applyIfJoinable(j: Join, l: LogicalRelation,
//                              r: JoinedLogicalRelation): LogicalPlan = {
//    l.relation match {
//      case left: CanPushJoinsRelation =>
//        val logicalRelLeaves = r.collectLeaves()
//            .filter(_.isInstanceOf[LogicalRelation])
//            .map(_.asInstanceOf[LogicalRelation])
//        if ( logicalRelLeaves.forall(x => left.canJoinScan(x.relation)) ) {
//          JoinedLogicalRelation(l, r, j.joinType, j.condition)
//        } else {
//          j
//        }
//      case _ => j
//    }
//  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case Filter(cond1, _ @ Filter(cond2, child)) =>
      FilterCollection(cond1 :: cond2 :: Nil, child)
    case Filter(cond, _ @ FilterCollection(f, child)) =>
      FilterCollection(f :+ cond, child)
    case FilterCollection(conds, _ @ Filter(f, child)) =>
      FilterCollection(conds :+ f, child)
    case j: Join =>
      val p = applyIfJoinable(j)
      p
  }
}

// scalastyle:on println
