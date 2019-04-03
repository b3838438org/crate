/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.planner.operators;

import io.crate.planner.TableStats;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Before;
import org.junit.Test;

import static io.crate.planner.operators.LogicalPlannerTest.isPlan;

public class WindowAggTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void init() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("create table t1 (x int, y int)")
            .build();
    }

    private LogicalPlan plan(String statement) {
        return LogicalPlannerTest.plan(statement, e, clusterService, new TableStats());
    }

    @Test
    public void testTwoWindowFunctionsWithDifferentWindowDefinitionResultsInTwoOperators() {
        LogicalPlan plan = plan("select avg(x) over (order by x), avg(x) over (order by y) from t1");
        String expectedPlan =
            "FetchOrEval[avg(x), avg(x)]\n" +
            "WindowAgg[avg(x) | ORDER BY Ref{doc.t1.y, integer} ASC]\n" +
            "WindowAgg[avg(x) | ORDER BY Ref{doc.t1.x, integer} ASC]\n" +
            "Collect[doc.t1 | [x, y] | All]\n";
        assertThat(plan, isPlan(e.functions(), expectedPlan));
    }
}
