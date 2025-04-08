#!/bin/bash

# 设置测试总次数（默认为200，可以通过命令行参数修改）
TOTAL_TESTS=${1:-200}  # 如果没有提供参数，默认为200
PARALLEL_JOBS=1      # 固定并行任务数为35

# 获取当前目录
current_dir=$(pwd)
echo "Current directory: $current_dir"  # 调试日志

# 创建一个时间戳目录来存储所有输出 (在当前目录下)
timestamp=$(date +%Y%m%d_%H%M%S)
echo "Timestamp: $timestamp"  # 调试日志
output_dir="${current_dir}/${timestamp}_test_results"
echo "Output directory: $output_dir"  # 调试日志

# 如果创建目录失败，则退出
if ! mkdir -p "$output_dir"; then
    echo "Error: Cannot create output directory '$output_dir'" >&2
    exit 1
fi

# 定义所有测试用例的列表
TESTS=(
    "TestBasic3A"
    "TestSpeed3A"
    "TestConcurrent3A"
    "TestUnreliable3A"
    "TestUnreliableOneKey3A"
    "TestOnePartition3A"
    "TestManyPartitionsOneClient3A"
    "TestManyPartitionsManyClients3A"
    "TestPersistOneClient3A"
    "TestPersistConcurrent3A"
    "TestPersistConcurrentUnreliable3A"
    "TestPersistPartition3A"
    "TestPersistPartitionUnreliable3A"
    "TestPersistPartitionUnreliableLinearizable3A"
    "TestSnapshotRPC3B"
    "TestSnapshotSize3B"
    "TestSpeed3B"
    "TestSnapshotRecover3B"
    "TestSnapshotRecoverManyClients3B"
    "TestSnapshotUnreliable3B"
    "TestSnapshotUnreliableRecover3B"
    "TestSnapshotUnreliableRecoverConcurrentPartition3B"
    "TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B"
)
echo "TESTS array before run_test_batch: ${TESTS[@]}"  # 调试日志
echo "Number of tests before run_test_batch: ${#TESTS[@]}"  # 调试日志

# 创建运行测试的函数
run_test_batch() {
    local batch_num=$1
    local output_dir=$2
    local tests_str=$3  # 接收传递的测试用例字符串

    # 将字符串转换回数组
    IFS=' ' read -r -a TESTS <<< "$tests_str"

    echo "Inside run_test_batch: batch_num=$batch_num, output_dir=$output_dir"  # 调试日志
    echo "TESTS array in run_test_batch: ${TESTS[@]}"  # 调试日志
    echo "Number of tests in run_test_batch: ${#TESTS[@]}"  # 调试日志

    local batch_dir="${output_dir}/batch_${batch_num}"
    echo "Batch directory: $batch_dir"  # 调试日志
    if ! mkdir -p "$batch_dir"; then
        echo "Error: Cannot create batch directory '$batch_dir'" >&2
        return 1
    fi

    echo "Running batch $batch_num with ${#TESTS[@]} tests"

    # 记录失败的测试用例
    local failed_tests=()

    for test_name in "${TESTS[@]}"; do
        echo "Starting test $test_name in batch $batch_num"
        local test_timestamp=$(date +%Y%m%d_%H%M%S)
        local batch_output="${batch_dir}/kvraft_${test_name}_${test_timestamp}.ansi"
        echo "Output file: $batch_output"  # 调试日志

        echo -e "Batch $batch_num started for $test_name" > "$batch_output"

        echo "Running: go test -v -race -run ^${test_name}$"
        if ! go test -v -race -run "^${test_name}$" >> "$batch_output" 2>&1; then
            echo -e "\nBatch $batch_num failed for $test_name" >> "$batch_output"
            failed_tests+=("$test_name")
        else
            echo -e "\nBatch $batch_num passed for $test_name" >> "$batch_output"
        fi

        echo -e "\nBatch $batch_num finished for $test_name" >> "$batch_output"
        echo -e "\n********************************************************************************" >> "$batch_output"
        echo -e "\n" >> "$batch_output"
    done

    # 报告失败的测试用例
    if [ ${#failed_tests[@]} -ne 0 ]; then
        echo "Batch $batch_num: The following tests failed: ${failed_tests[@]}" >&2
    else
        echo "Batch $batch_num: All tests passed successfully"
    fi

    # 始终返回 0，确保 parallel 继续运行其他批次
    return 0
}

export -f run_test_batch
export output_dir  # 导出环境变量

# 处理脚本中断 (例如 Ctrl+C)
trap 'rm -rf "$output_dir"; exit 1' INT

# 并行运行测试
echo "Starting parallel execution with $TOTAL_TESTS batches"  # 调试日志
tests_str="${TESTS[*]}"  # 将数组转换为字符串
seq 1 "$TOTAL_TESTS" | parallel --bar --jobs=$PARALLEL_JOBS --quote run_test_batch {} "$output_dir" "$tests_str"

# 修改合并部分的代码
echo "Merging test results..."

echo "All tests completed. Results saved in ${output_dir}/${timestamp}_combined.ansi"