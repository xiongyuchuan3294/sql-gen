#!/bin/bash
# Check Size (Batch)
hadoop fs -du -h /user/hive/warehouse/hduser1009/imd_aml_safe.db/example_table_A/ds=2025-01-01;
hadoop fs -du -h /user/hive/warehouse/hduser1009/imd_aml_safe.db/example_table_B/ds=2025-01-02;
hadoop fs -du -h /user/hive/warehouse/custom/path;
