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
package org.apache.iceberg.actions;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteJobOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.actions.RewriteDataFiles.FileGroupInfo;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.StructLikeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Groups specified data files in the {@link Table} into {@link RewriteFileGroup}s. The files are
 * grouped by partitions based on their size using fix sized bins. Extends {@link
 * SizeBasedFileRewritePlanner} with delete file number and delete ratio thresholds and job {@link
 * RewriteDataFiles#REWRITE_JOB_ORDER} handling.
 */
public class BinPackRewriteFilePlanner
    extends SizeBasedFileRewritePlanner<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> {
  /**
   * The minimum number of deletes that needs to be associated with a data file for it to be
   * considered for rewriting. If a data file has this number of deletes or more, it will be
   * rewritten regardless of its file size determined by {@link #MIN_FILE_SIZE_BYTES} and {@link
   * #MAX_FILE_SIZE_BYTES}. If a file group contains a file that satisfies this condition, the file
   * group will be rewritten regardless of the number of files in the file group determined by
   * {@link #MIN_INPUT_FILES}.
   *
   * <p>Defaults to Integer.MAX_VALUE, which means this feature is not enabled by default.
   */
  public static final String DELETE_FILE_THRESHOLD = "delete-file-threshold";

  public static final int DELETE_FILE_THRESHOLD_DEFAULT = Integer.MAX_VALUE;

  /**
   * The ratio of the deleted rows in a data file for it to be considered for rewriting. If the
   * deletion ratio of a data file is greater than or equal to this value, it will be rewritten
   * regardless of its file size determined by {@link #MIN_FILE_SIZE_BYTES} and {@link
   * #MAX_FILE_SIZE_BYTES}. If a file group contains a file that satisfies this condition, the file
   * group will be rewritten regardless of the number of files in the file group determined by
   * {@link #MIN_INPUT_FILES}.
   *
   * <p>Defaults to 0.3, which means that if the number of deleted records in a file reaches or
   * exceeds 30%, it will trigger the rewriting operation.
   */
  public static final String DELETE_RATIO_THRESHOLD = "delete-ratio-threshold";

  public static final double DELETE_RATIO_THRESHOLD_DEFAULT = 0.3;

  /**
   * The max number of files to be rewritten (Not providing this value would rewrite all the files)
   */
  public static final String MAX_FILES_TO_REWRITE = "max-files-to-rewrite";

  private static final Logger LOG = LoggerFactory.getLogger(BinPackRewriteFilePlanner.class);

  private final Expression filter;
  private final Long snapshotId;
  private final boolean caseSensitive;

  private int deleteFileThreshold;
  private double deleteRatioThreshold;
  private RewriteJobOrder rewriteJobOrder;
  private Integer maxFilesToRewrite;

  public BinPackRewriteFilePlanner(Table table) {
    this(table, Expressions.alwaysTrue());
  }

  public BinPackRewriteFilePlanner(Table table, Expression filter) {
    this(
        table,
        filter,
        table.currentSnapshot() != null ? table.currentSnapshot().snapshotId() : null,
        false);
  }

  /**
   * Creates the planner for the given table.
   *
   * @param table to plan for
   * @param filter used to remove files from the plan
   * @param snapshotId a snapshot ID used for planning and as the starting snapshot id for commit
   *     validation when replacing the files
   * @param caseSensitive property used for scanning
   */
  public BinPackRewriteFilePlanner(
      Table table, Expression filter, Long snapshotId, boolean caseSensitive) {
    super(table);
    this.filter = filter;
    this.snapshotId = snapshotId;
    this.caseSensitive = caseSensitive;
  }

  @Override
  public Set<String> validOptions() {
    return ImmutableSet.<String>builder()
        .addAll(super.validOptions())
        .add(DELETE_FILE_THRESHOLD)
        .add(DELETE_RATIO_THRESHOLD)
        .add(RewriteDataFiles.REWRITE_JOB_ORDER)
        .add(MAX_FILES_TO_REWRITE)
        .build();
  }

  @Override
  public void init(Map<String, String> options) {
    super.init(options);
    this.deleteFileThreshold = deleteFileThreshold(options);
    this.deleteRatioThreshold = deleteRatioThreshold(options);
    this.rewriteJobOrder =
        RewriteJobOrder.fromName(
            PropertyUtil.propertyAsString(
                options,
                RewriteDataFiles.REWRITE_JOB_ORDER,
                RewriteDataFiles.REWRITE_JOB_ORDER_DEFAULT));
    this.maxFilesToRewrite = maxFilesToRewrite(options);
  }

  private int deleteFileThreshold(Map<String, String> options) {
    int value =
        PropertyUtil.propertyAsInt(options, DELETE_FILE_THRESHOLD, DELETE_FILE_THRESHOLD_DEFAULT);
    Preconditions.checkArgument(
        value >= 0, "'%s' is set to %s but must be >= 0", DELETE_FILE_THRESHOLD, value);
    return value;
  }

  private double deleteRatioThreshold(Map<String, String> options) {
    double value =
        PropertyUtil.propertyAsDouble(
            options, DELETE_RATIO_THRESHOLD, DELETE_RATIO_THRESHOLD_DEFAULT);
    Preconditions.checkArgument(
        value > 0, "'%s' is set to %s but must be > 0", DELETE_RATIO_THRESHOLD, value);
    Preconditions.checkArgument(
        value <= 1, "'%s' is set to %s but must be <= 1", DELETE_RATIO_THRESHOLD, value);
    return value;
  }

  private Integer maxFilesToRewrite(Map<String, String> options) {
    Integer value = PropertyUtil.propertyAsNullableInt(options, MAX_FILES_TO_REWRITE);
    Preconditions.checkArgument(
        value == null || value > 0,
        "Cannot set %s to %s, the value must be positive integer.",
        MAX_FILES_TO_REWRITE,
        value);
    return value;
  }

  @Override
  protected Iterable<FileScanTask> filterFiles(Iterable<FileScanTask> tasks) {
    return Iterables.filter(
        tasks,
        task ->
            outsideDesiredFileSizeRange(task) || tooManyDeletes(task) || tooHighDeleteRatio(task));
  }

  @Override
  protected Iterable<List<FileScanTask>> filterFileGroups(List<List<FileScanTask>> groups) {
    return Iterables.filter(
        groups,
        group ->
            enoughInputFiles(group)
                || enoughContent(group)
                || tooMuchContent(group)
                || group.stream().anyMatch(this::tooManyDeletes)
                || group.stream().anyMatch(this::tooHighDeleteRatio));
  }

  @Override
  protected long defaultTargetFileSize() {
    return PropertyUtil.propertyAsLong(
        table().properties(),
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
  }

  @Override
  public FileRewritePlan<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> plan() {
    Map<Integer, StructLikeMap<List<List<FileScanTask>>>> plan = planFileGroups();
    RewriteExecutionContext ctx = new RewriteExecutionContext();
    List<RewriteFileGroup> selectedFileGroups = Lists.newArrayList();
    AtomicInteger fileCountRunner = new AtomicInteger();

    plan.forEach(
        (specId, partitionMap) ->
            partitionMap.entrySet().stream()
                .filter(e -> !e.getValue().isEmpty())
                .forEach(
                    entry -> {
                      StructLike partition = entry.getKey();
                      entry
                          .getValue()
                          .forEach(
                              fileScanTasks -> {
                                long inputSize = inputSize(fileScanTasks);
                                if (maxFilesToRewrite == null) {
                                  selectedFileGroups.add(
                                      newRewriteGroup(
                                          ctx,
                                          partition,
                                          fileScanTasks,
                                          inputSplitSize(inputSize),
                                          expectedOutputFiles(inputSize),
                                          specId));
                                } else if (fileCountRunner.get() < maxFilesToRewrite) {
                                  int remainingSize = maxFilesToRewrite - fileCountRunner.get();
                                  int scanTasksToRewrite =
                                      Math.min(fileScanTasks.size(), remainingSize);
                                  List<FileScanTask> tasksToRewrite =
                                      fileScanTasks.subList(0, scanTasksToRewrite);
                                  long rewriteInputSize = inputSize(tasksToRewrite);
                                  selectedFileGroups.add(
                                      newRewriteGroup(
                                          ctx,
                                          partition,
                                          tasksToRewrite,
                                          inputSplitSize(rewriteInputSize),
                                          expectedOutputFiles(rewriteInputSize),
                                          specId));
                                  fileCountRunner.getAndAdd(scanTasksToRewrite);
                                }
                              });
                    }));

    int totalGroupCount =
        plan.values().stream()
            .mapToInt(m -> m.values().stream().mapToInt(List::size).sum())
            .sum();
    // Build groupsInPartition across all specs using current spec's partition type for the map
    StructLikeMap<Integer> groupsInPartition =
        StructLikeMap.create(table().spec().partitionType());
    plan.forEach(
        (specId, partitionMap) ->
            partitionMap.forEach(
                (partition, groups) ->
                    groupsInPartition.merge(partition, groups.size(), Integer::sum)));

    return new FileRewritePlan<>(
        CloseableIterable.of(
            selectedFileGroups.stream()
                .sorted(RewriteFileGroup.comparator(rewriteJobOrder))
                .collect(Collectors.toList())),
        totalGroupCount,
        groupsInPartition);
  }

  private boolean tooManyDeletes(FileScanTask task) {
    return task.deletes() != null && task.deletes().size() >= deleteFileThreshold;
  }

  private boolean tooHighDeleteRatio(FileScanTask task) {
    if (task.deletes() == null || task.deletes().isEmpty()) {
      return false;
    }

    long knownDeletedRecordCount =
        task.deletes().stream()
            .filter(ContentFileUtil::isFileScoped)
            .mapToLong(ContentFile::recordCount)
            .sum();

    double deletedRecords = (double) Math.min(knownDeletedRecordCount, task.file().recordCount());
    double deleteRatio = deletedRecords / task.file().recordCount();
    return deleteRatio >= deleteRatioThreshold;
  }

  private Map<Integer, StructLikeMap<List<List<FileScanTask>>>> planFileGroups() {
    TableScan scan =
        table().newScan().filter(filter).caseSensitive(caseSensitive).ignoreResiduals();

    if (snapshotId != null) {
      scan = scan.useSnapshot(snapshotId);
    }

    CloseableIterable<FileScanTask> fileScanTasks = scan.planFiles();

    try {
      if (useInputSpec()) {
        return groupBySpecAndPartition(table(), fileScanTasks);
      } else {
        Types.StructType partitionType = table().spec().partitionType();
        StructLikeMap<List<FileScanTask>> filesByPartition =
            groupByPartition(table(), partitionType, fileScanTasks);
        StructLikeMap<List<List<FileScanTask>>> planned =
            filesByPartition.transformValues(
                tasks -> ImmutableList.copyOf(planFileGroups(tasks)));
        // Wrap in a single-entry map keyed by the global outputSpecId
        Map<Integer, StructLikeMap<List<List<FileScanTask>>>> result = Maps.newHashMap();
        result.put(outputSpecId(), planned);
        return result;
      }
    } finally {
      try {
        fileScanTasks.close();
      } catch (IOException io) {
        LOG.error("Cannot properly close file iterable while planning for rewrite", io);
      }
    }
  }

  /**
   * Groups files by their source partition spec, then by partition within each spec. This preserves
   * per-spec partition boundaries so that compaction writes output files back using the same spec as
   * their inputs, preventing partition evolution from increasing the file count.
   */
  private Map<Integer, StructLikeMap<List<List<FileScanTask>>>> groupBySpecAndPartition(
      Table table, Iterable<FileScanTask> tasks) {
    // Collect files per spec, then per partition within that spec
    Map<Integer, StructLikeMap<List<FileScanTask>>> filesBySpecAndPartition = Maps.newHashMap();

    for (FileScanTask task : tasks) {
      int specId = task.file().specId();
      PartitionSpec spec = table.specs().get(specId);
      if (spec == null) {
        // Fallback: treat as current spec if spec is missing (shouldn't happen in practice)
        spec = table.spec();
        specId = spec.specId();
      }
      final int effectiveSpecId = specId;
      final PartitionSpec effectiveSpec = spec;
      StructLikeMap<List<FileScanTask>> partitionMap =
          filesBySpecAndPartition.computeIfAbsent(
              effectiveSpecId,
              id -> StructLikeMap.create(effectiveSpec.partitionType()));
      partitionMap.computeIfAbsent(task.file().partition(), unused -> Lists.newArrayList()).add(task);
    }

    // Plan file groups within each (spec, partition) bucket
    Map<Integer, StructLikeMap<List<List<FileScanTask>>>> result = Maps.newHashMap();
    filesBySpecAndPartition.forEach(
        (specId, partitionMap) -> {
          StructLikeMap<List<List<FileScanTask>>> planned =
              partitionMap.transformValues(
                  specTasks -> ImmutableList.copyOf(planFileGroups(specTasks)));
          result.put(specId, planned);
        });
    return result;
  }

  private StructLikeMap<List<FileScanTask>> groupByPartition(
      Table table, Types.StructType partitionType, Iterable<FileScanTask> tasks) {
    StructLikeMap<List<FileScanTask>> filesByPartition = StructLikeMap.create(partitionType);
    StructLike emptyStruct = GenericRecord.create(partitionType);

    for (FileScanTask task : tasks) {
      // If a task uses an incompatible partition spec the data inside could contain values
      // which belong to multiple partitions in the current spec. Treating all such files as
      // un-partitioned and grouping them together helps to minimize new files made.
      StructLike taskPartition =
          task.file().specId() == table.spec().specId() ? task.file().partition() : emptyStruct;

      filesByPartition.computeIfAbsent(taskPartition, unused -> Lists.newArrayList()).add(task);
    }

    return filesByPartition;
  }

  private RewriteFileGroup newRewriteGroup(
      RewriteExecutionContext ctx,
      StructLike partition,
      List<FileScanTask> tasks,
      long inputSplitSize,
      int expectedOutputFiles,
      int specId) {
    FileGroupInfo info =
        ImmutableRewriteDataFiles.FileGroupInfo.builder()
            .globalIndex(ctx.currentGlobalIndex())
            .partitionIndex(ctx.currentPartitionIndex(partition))
            .partition(partition)
            .build();
    return new RewriteFileGroup(
        info,
        Lists.newArrayList(tasks),
        specId,
        writeMaxFileSize(),
        inputSplitSize,
        expectedOutputFiles);
  }
}
