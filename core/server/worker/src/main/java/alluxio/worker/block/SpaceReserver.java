/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.block;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.Sessions;
import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.heartbeat.HeartbeatExecutor;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The {@link SpaceReserver} periodically checks the available space on each storage tier. If the
 * used space is above the high watermark configured for the tier, eviction will be triggered to
 * reach the low watermark. If this is not the top tier, it will also add the amount of space
 * reserved on the tier above to reduce the impact of cascading eviction.
 */
@NotThreadSafe
public class SpaceReserver implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(SpaceReserver.class);

  /** The block worker the space reserver monitors. */
  private final BlockWorker mBlockWorker;

  /** Association between storage tier aliases and ordinals for the worker. */
  private final StorageTierAssoc mStorageTierAssoc;

  /** Mapping from tier alias to high watermark in bytes. */
  private final Map<String, Long> mHighWatermarks = new HashMap<>();

  /** Mapping from tier alias to space in bytes to be reserved on the tier. */
  private final Map<String, Long> mReservedSpaces = new HashMap<>();

  /**
   * Creates a new instance of {@link SpaceReserver}.
   *
   * @param blockWorker the block worker handle
   */
  public SpaceReserver(BlockWorker blockWorker) {
    mBlockWorker = blockWorker;
    mStorageTierAssoc = new WorkerStorageTierAssoc();
    Map<String, Long> tierCapacities = blockWorker.getStoreMeta().getCapacityBytesOnTiers();
    long lastTierReservedBytes = 0;
    for (int ordinal = 0; ordinal < mStorageTierAssoc.size(); ordinal++) {
      String tierAlias = mStorageTierAssoc.getAlias(ordinal);
      long tierCapacity = tierCapacities.get(tierAlias);
      long reservedSpace;
      PropertyKey tierReservedSpaceProp =
          PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO.format(ordinal);
      if (Configuration.containsKey(tierReservedSpaceProp)) {
        LOG.warn("The property reserved.ratio is deprecated, use high/low watermark instead.");
        reservedSpace = (long) (tierCapacity * Configuration.getDouble(tierReservedSpaceProp));
      } else {
        // High watermark defines when to start the space reserving process
        PropertyKey tierHighWatermarkProp =
            PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_HIGH_WATERMARK_RATIO.format(ordinal);
        Preconditions.checkArgument(Configuration.getDouble(tierHighWatermarkProp) > 0,
            "The high watermark of tier " + ordinal + " should be positive");
        Preconditions.checkArgument(Configuration.getDouble(tierHighWatermarkProp) < 1,
            "The high watermark of tier " + ordinal + " should be less than 1.0");
        long highWatermark =
            (long) (tierCapacity * Configuration.getDouble(tierHighWatermarkProp));
        mHighWatermarks.put(tierAlias, highWatermark);

        // Low watermark defines when to stop the space reserving process if started
        PropertyKey tierLowWatermarkProp =
            PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_LOW_WATERMARK_RATIO.format(ordinal);
        Preconditions.checkArgument(Configuration.getDouble(tierLowWatermarkProp) >= 0,
            "The low watermark of tier " + ordinal + " should not be negative");
        reservedSpace =
            (long) (tierCapacity - tierCapacity * Configuration.getDouble(tierLowWatermarkProp));
      }
      mReservedSpaces.put(tierAlias, reservedSpace + lastTierReservedBytes);
      lastTierReservedBytes += reservedSpace;
    }
  }

  private void reserveSpace() {
    Map<String, Long> usedBytesOnTiers = mBlockWorker.getStoreMeta().getUsedBytesOnTiers();
    for (int ordinal = mStorageTierAssoc.size() - 1; ordinal >= 0; ordinal--) {
      String tierAlias = mStorageTierAssoc.getAlias(ordinal);
      long reservedSpace = mReservedSpaces.get(tierAlias);
      if (mHighWatermarks.containsKey(tierAlias)) {
        long highWatermark = mHighWatermarks.get(tierAlias);
        if (highWatermark > reservedSpace && usedBytesOnTiers.get(tierAlias) >= highWatermark) {
          try {
            mBlockWorker.freeSpace(Sessions.MIGRATE_DATA_SESSION_ID, reservedSpace, tierAlias);
          } catch (WorkerOutOfSpaceException | BlockDoesNotExistException
              | BlockAlreadyExistsException | InvalidWorkerStateException | IOException e) {
            LOG.warn("SpaceReserver failed to free tier {} to {} bytes used", tierAlias,
                reservedSpace, e.getMessage());
          }
        }
      } else {
        try {
          mBlockWorker.freeSpace(Sessions.MIGRATE_DATA_SESSION_ID, reservedSpace, tierAlias);
        } catch (WorkerOutOfSpaceException | BlockDoesNotExistException
            | BlockAlreadyExistsException | InvalidWorkerStateException | IOException e) {
          LOG.warn(e.getMessage());
        }
      }
    }
  }

  @Override
  public void heartbeat() {
    reserveSpace();
  }

  @Override
  public void close() {
    // Nothing to close.
  }
}
