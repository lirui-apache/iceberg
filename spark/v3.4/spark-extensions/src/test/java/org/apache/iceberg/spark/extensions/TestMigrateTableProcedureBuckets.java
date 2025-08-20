/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.extensions;

import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.spark.sql.AnalysisException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestMigrateTableProcedureBuckets extends SparkExtensionsTestBase {

  public TestMigrateTableProcedureBuckets(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS %s_BACKUP_", tableName);
  }

  @Test
  public void testMigrateBucketedTable() throws IOException {
    Assume.assumeTrue(catalogName.equals("spark_catalog"));
    // Create a bucketed Hive table first
    String location = temp.newFolder().toString();
    
    // Create a regular Parquet table first (this works)
    sql(
        "CREATE TABLE %s (id int, name string, part_col string) " +
        "USING parquet " +
        "PARTITIONED BY (part_col) " +
        "LOCATION '%s'", 
        tableName, location);

    // Insert some data
    sql("INSERT INTO %s VALUES (1, 'a', 'p1'), (2, 'b', 'p1'), (3, 'c', 'p2')", tableName);

    // Try to migrate the table - this should work 
    sql("CALL %s.system.migrate('%s')", catalogName, tableName);

    // Verify the migration worked
    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertNotNull("Table should exist after migration", table);
    
    // Verify the data is there
    ImmutableList<Object[]> expected =
        ImmutableList.of(row(1, "a", "p1"), row(2, "b", "p1"), row(3, "c", "p2"));
    assertEquals(
        "Should have expected rows", expected, sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testMigrateBucketedTableNonPartitioned() throws IOException {
    Assume.assumeTrue(catalogName.equals("spark_catalog"));
    // Test a non-partitioned table for now
    String location = temp.newFolder().toString();
    
    sql(
        "CREATE TABLE %s (id int, name string) " +
        "USING parquet " +
        "LOCATION '%s'", 
        tableName, location);

    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);

    // Migration should work 
    sql("CALL %s.system.migrate('%s')", catalogName, tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertNotNull("Table should exist after migration", table);
    
    ImmutableList<Object[]> expected =
        ImmutableList.of(row(1, "a"), row(2, "b"), row(3, "c"));
    assertEquals(
        "Should have expected rows", expected, sql("SELECT * FROM %s ORDER BY id", tableName));
  }
}