/*
 * Licensed to CRATE.IO GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.integrationtests;

import io.crate.testing.UseJdbc;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.disruption.BusyMasterServiceDisruption;
import org.elasticsearch.test.disruption.ServiceDisruptionScheme;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
@UseJdbc(0) // missing column types
public class SnapshotResiliencyTest extends SQLTransportIntegrationTest {


    @ClassRule
    public static TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

    private static final String REPOSITORY_NAME = "test_snapshots_repo";
    private static final String TABLE_NAME = "test_table";
    private static final String SNAPSHOT_NAME = "test_snapshot";

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("path.repo", TEMP_FOLDER.getRoot().getAbsolutePath())
            .build();
    }

    @Before
    public void createRepository() throws Exception {
        internalCluster().startMasterOnlyNode(Settings.EMPTY);
        internalCluster().startDataOnlyNodes(randomIntBetween(2, 5));

        execute("create repository " + REPOSITORY_NAME + " TYPE \"fs\" with (location=?, compress=True)",
            new Object[]{TEMP_FOLDER.newFolder().getAbsolutePath()});
        waitNoPendingTasksOnAll();
        assertThat(response.rowCount(), is(1L));

        createTableWithData(TABLE_NAME);
    }

    private void createTableWithData(String tableName) {
        execute("create table " + tableName + " (id integer primary key)");
        ensureYellow();

        int bulkSize = randomIntBetween(100, 250);
        Object[][] bulkArgs = new Object[bulkSize][];
        for (int i = 0; i < bulkSize; i++) {
            bulkArgs[i] = new Object[]{i};
        }
        execute("insert into " + tableName + " (id) values (?)", bulkArgs);
        refresh();
    }

    @Test
    public void testDataNodeRestartWithBusyMasterDuringSnapshot() throws Exception {
        ServiceDisruptionScheme disruption = new BusyMasterServiceDisruption(random(), Priority.HIGH);

        execute("create snapshot " + REPOSITORY_NAME + "." + SNAPSHOT_NAME +
            " TABLE " + TABLE_NAME + " with (wait_for_completion=false)");

        setDisruptionScheme(disruption);
        disruption.startDisrupting();
        internalCluster().restartRandomDataNode(InternalTestCluster.EMPTY_CALLBACK);
        disruption.stopDisrupting();

        assertBusy(() -> {
            execute("select * from sys.snapshots");
            assertThat(response.rows().length, is(1));
        }, 30, TimeUnit.SECONDS);
    }

    public void testSnapshotWithNodeShutdown() throws Exception {
        execute("create snapshot " + REPOSITORY_NAME + "." + SNAPSHOT_NAME +
            " TABLE " + TABLE_NAME + " with (wait_for_completion=false)");

        internalCluster().stopRandomDataNode();

        assertBusy(() -> {
            execute("select * from sys.snapshots");
            assertThat(response.rows().length, is(1));
        }, 30, TimeUnit.SECONDS);
    }

    @After
    public void cleanUp() {
        execute("DROP REPOSITORY " + REPOSITORY_NAME);
        assertThat(response.rowCount(), is(1L));
    }
}
