#!/bin/bash

# 运行本脚本的命令 bash check_data_num.sh

# 设置起始和结束日期
start_date="2024-09-23"
end_date="2024-09-28"

# 设置数据量阈值（以字节为单位）
threshold=6200  # 例如 设置为1000000代表1MB

# 将起始和结束日期转换为时间戳
start_ts=$(date -d "$start_date" +%s)
end_ts=$(date -d "$end_date" +%s)

# 循环检查每一天的分区
current_ts=$start_ts
while [ $current_ts -le $end_ts ]; do
    # 将时间戳转换回日期格式
    current_date=$(date -d "@$current_ts" +%Y-%m-%d)
    partition_path="/user/hive/warehouse/hduser1009/imd_aml_safe.db/rrs_aml_base_ccust_info_fact/ds=$current_date"

    echo "Checking partition: $partition_path"

    # 获取分区的大小
    data_size=$(hdfs dfs -du -s $partition_path | awk '{print $1}')

    if [ -z "$data_size" ]; then
        echo "Partition $partition_path check data failed ,does not exist."
    else
        echo "Data size for partition $partition_path: $data_size bytes"
        if [ "$data_size" -gt "$threshold" ]; then
            echo "Partition $partition_path check data pass"
        else
            echo "Partition $partition_path check data failed ,less than $threshold bytes)."
        fi
    fi

    # 增加一天
    current_ts=$(($current_ts + 86400))  # 86400 秒 = 1 天
done