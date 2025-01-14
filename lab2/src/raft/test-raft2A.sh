#!/bin/bash

# 获取当前时间作为日志文件名的一部分
timestamp=$(date +%Y%m%d_%H%M%S)
logfile="raft_test_${timestamp}.log"

echo "Starting Raft tests, logging to ${logfile}"
echo "Running 200 iterations..."

# 运行测试200次，并将输出同时显示到终端和保存到日志文件
for i in {1..200}; do
    echo "=== Test iteration $i ===" | tee -a "${logfile}"
    go test -run 2A 2>&1 | tee -a "${logfile}"

    # 记录测试结果但继续执行
    if [ ${PIPESTATUS[0]} -ne 0 ]; then
        echo "Test failed at iteration $i, continuing..." | tee -a "${logfile}"
    fi
done

echo "All tests completed. Log saved to ${logfile}"