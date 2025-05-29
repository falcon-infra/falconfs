/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#pragma once

#include <vector>

#include <prometheus/gauge.h>
#include <prometheus/registry.h>
#include <prometheus/exposer.h>

#include "stats/falcon_stats.h"
#include "connection/node.h"
#include "log/logging.h"
#include "falcon_store/falcon_store.h"

int startPrometheusMonitor(const std::string &endpoint, std::stop_token stoken)
{
    static prometheus::Exposer exposer(endpoint);
    auto registry = std::make_shared<prometheus::Registry>();

    // Create a gauge metric
    auto &ops = prometheus::BuildGauge()
                      .Name("observed_IOPS")
                      .Help("Observed IOPS for FUSE operations")
                      .Register(*registry);

    // Update the gauge with current stats
    auto &overall_ops = ops.Add({{"category", "overall"}, {"name", "overall-ops"}});
    auto &read_ops = ops.Add({{"category", "data"}, {"name", "read-ops"}});
    auto &write_ops = ops.Add({{"category", "data"}, {"name", "write-ops"}});
    auto &meta_ops = ops.Add({{"category", "meta"}, {"name", "meta-ops"}});
    auto &open_ops = ops.Add({{"category", "meta"}, {"name", "open-ops"}});
    auto &close_ops = ops.Add({{"category", "meta"}, {"name", "close-ops"}});
    auto &stat_ops = ops.Add({{"category", "meta"}, {"name", "stat-ops"}});
    auto &create_ops = ops.Add({{"category", "meta"}, {"name", "create-ops"}});
    auto &unlink_ops = ops.Add({{"category", "meta"}, {"name", "unlink-ops"}});
    auto &mkdir_ops = ops.Add({{"category", "meta"}, {"name", "mkdir-ops"}});
    auto &rmdir_ops = ops.Add({{"category", "meta"}, {"name", "rmdir-ops"}});
    auto &readdir_ops = ops.Add({{"category", "meta"}, {"name", "readdir-ops"}});
    auto &rename_ops = ops.Add({{"category", "meta"}, {"name", "rename-ops"}});
    auto &access_ops = ops.Add({{"category", "meta"}, {"name", "access-ops"}});
    auto &truncate_ops = ops.Add({{"category", "meta"}, {"name", "truncate-ops"}});
    auto &flush_ops = ops.Add({{"category", "meta"}, {"name", "flush-ops"}});
    auto &fsync_ops = ops.Add({{"category", "meta"}, {"name", "fsync-ops"}});

    // latency metrics
    auto &latency = prometheus::BuildGauge()
                        .Name("observed_latency")
                        .Help("Observed latency for FUSE operations")
                        .Register(*registry);
    auto &overall_latency = latency.Add({{"category", "overall"}, {"name", "overall-latency"}});
    auto &read_latency = latency.Add({{"category", "data"}, {"name", "read-latency"}});
    auto &write_latency = latency.Add({{"category", "data"}, {"name", "write-latency"}});
    auto &meta_latency = latency.Add({{"category", "meta"}, {"name", "meta-latency"}});
    auto &open_latency = latency.Add({{"category", "meta"}, {"name", "open-latency"}});
    auto &close_latency = latency.Add({{"category", "meta"}, {"name", "close-latency"}});
    auto &stat_latency = latency.Add({{"category", "meta"}, {"name", "stat-latency"}});
    auto &create_latency = latency.Add({{"category", "meta"}, {"name", "create-latency"}});

    auto &overall_latency_max = latency.Add({{"category", "overall"}, {"name", "overall-latency-max"}});
    auto &read_latency_max = latency.Add({{"category", "data"}, {"name", "read-latency-max"}});
    auto &write_latency_max = latency.Add({{"category", "data"}, {"name", "write-latency-max"}});
    auto &meta_latency_max = latency.Add({{"category", "meta"}, {"name", "meta-latency-max"}});
    auto &open_latency_max = latency.Add({{"category", "meta"}, {"name", "open-latency-max"}});
    auto &close_latency_max = latency.Add({{"category", "meta"}, {"name", "close-latency-max"}});
    auto &stat_latency_max = latency.Add({{"category", "meta"}, {"name", "stat-latency-max"}});
    auto &create_latency_max = latency.Add({{"category", "meta"}, {"name", "create-latency-max"}});

    // throughput metrics
    auto &throughput = prometheus::BuildGauge()
                           .Name("observed_throughput")
                           .Help("Observed throughput for FUSE operations")
                           .Register(*registry);
    auto &overall_read_throughput = throughput.Add({{"category", "overall"}, {"name", "overall-read-throughput"}});
    auto &overall_write_throughput = throughput.Add({{"category", "overall"}, {"name", "overall-write-throughput"}});
    auto &blockcache_read_throughput = throughput.Add({{"category", "blockcache"}, {"name", "blockcache-read-throughput"}});
    auto &blockcache_write_throughput = throughput.Add({{"category", "blockcache"}, {"name", "blockcache-write-throughput"}});
    auto &object_read_throughput = throughput.Add({{"category", "object"}, {"name", "object-read-throughput"}});
    auto &object_write_throughput = throughput.Add({{"category", "object"}, {"name", "object-write-throughput"}});

    // Register the gauge with the registry
    exposer.RegisterCollectable(registry);

    // Update the metrics periodically
    std::vector<size_t> currentStats(STATS_END);
    // drop stale stats
    int ret = FalconStore::GetInstance()->StatCluster(-1, currentStats, false);
    while (!stoken.stop_requested()) {
        sleep(60);
        ret = FalconStore::GetInstance()->StatCluster(-1, currentStats, false);
        if (ret != 0) {
            FALCON_LOG(LOG_ERROR) << "StatCluster failed: " << strerror(-ret);
            continue;
        }
        overall_ops.Set(currentStats[FUSE_OPS]);
        read_ops.Set(currentStats[FUSE_READ_OPS]);
        write_ops.Set(currentStats[FUSE_WRITE_OPS]);
        meta_ops.Set(currentStats[META_OPS]);
        open_ops.Set(currentStats[META_OPEN]);
        close_ops.Set(currentStats[META_RELEASE]);
        stat_ops.Set(currentStats[META_STAT]);
        create_ops.Set(currentStats[META_CREATE]);
        unlink_ops.Set(currentStats[META_UNLINK]);
        mkdir_ops.Set(currentStats[META_MKDIR]);
        rmdir_ops.Set(currentStats[META_RMDIR]);
        readdir_ops.Set(currentStats[META_READDIR]);
        rename_ops.Set(currentStats[META_RENAME]);
        access_ops.Set(currentStats[META_ACCESS]);
        truncate_ops.Set(currentStats[META_TRUNCATE]);
        flush_ops.Set(currentStats[META_FLUSH]);
        fsync_ops.Set(currentStats[META_FSYNC]);

        overall_latency.Set(currentStats[FUSE_LAT]);
        read_latency.Set(currentStats[FUSE_READ_LAT]);
        write_latency.Set(currentStats[FUSE_WRITE_LAT]);
        meta_latency.Set(currentStats[META_LAT]);
        open_latency.Set(currentStats[META_OPEN_LAT]);
        close_latency.Set(currentStats[META_RELEASE_LAT]);
        stat_latency.Set(currentStats[META_STAT_LAT]);
        create_latency.Set(currentStats[META_CREATE_LAT]);

        overall_latency_max.Set(currentStats[FUSE_LAT_MAX]);
        read_latency_max.Set(currentStats[FUSE_READ_LAT_MAX]);
        write_latency_max.Set(currentStats[FUSE_WRITE_LAT_MAX]);
        meta_latency_max.Set(currentStats[META_LAT_MAX]);
        open_latency_max.Set(currentStats[META_OPEN_LAT_MAX]);
        close_latency_max.Set(currentStats[META_RELEASE_LAT_MAX]);
        stat_latency_max.Set(currentStats[META_STAT_LAT_MAX]);
        create_latency_max.Set(currentStats[META_CREATE_LAT_MAX]);

        overall_read_throughput.Set(currentStats[FUSE_READ]);
        overall_write_throughput.Set(currentStats[FUSE_WRITE]);
        blockcache_read_throughput.Set(currentStats[BLOCKCACHE_READ]);
        blockcache_write_throughput.Set(currentStats[BLOCKCACHE_WRITE]);
        object_read_throughput.Set(currentStats[OBJ_GET]);
        object_write_throughput.Set(currentStats[OBJ_PUT]);
    }

    return 0;
}