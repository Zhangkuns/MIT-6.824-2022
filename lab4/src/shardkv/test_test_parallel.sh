#!/bin/bash

# 参数1: 测试总次数 (默认为100)
TOTAL_TESTS=${1:-100}

# 参数2: 要运行的测试函数名称 (默认为 TestJoinLeave)
# 使用 ^ 和 $ 来确保精确匹配测试函数名
TEST_PATTERN="^${2:-TestJoinLeave}$"

# 参数3: 是否只保留失败的日志 (默认为 "false"。传入 "true", "TRUE", 或 "1" 来启用)
KEEP_ONLY_FAILED_INPUT="${3:-false}"
KEEP_ONLY_FAILED=false
if [[ "$KEEP_ONLY_FAILED_INPUT" == "true" || "$KEEP_ONLY_FAILED_INPUT" == "TRUE" || "$KEEP_ONLY_FAILED_INPUT" == "1" ]]; then
    KEEP_ONLY_FAILED=true
fi

PARALLEL_JOBS=${PARALLEL_JOBS:-32}      # 如果环境变量 PARALLEL_JOBS 未设置，则默认为16

# 获取当前目录
current_dir=$(pwd)

# 从测试函数名中提取一个简称用于目录名 (移除 ^ 和 $)
test_name_for_dir=$(echo "$TEST_PATTERN" | sed 's/\^//g' | sed 's/\$//g')
timestamp=$(date +%Y%m%d_%H%M%S)
output_dir_base="${current_dir}/${timestamp}_${test_name_for_dir}_results"

# 如果创建目录失败，则退出
if ! mkdir -p "$output_dir_base"; then
    echo "Error: Cannot create output directory '$output_dir_base'" >&2
    exit 1
fi

# 创建运行测试的函数
# 参数: 1:批次号, 2:输出目录, 3:要运行的测试函数模式 (带^$)
run_test_batch() {
    local batch_num=$1
    local current_output_dir=$2
    local test_function_to_run=$3

    local clean_test_name_for_file=$(echo "$test_function_to_run" | sed 's/\^//g' | sed 's/\$//g')
    local batch_output_file="${current_output_dir}/batch_${batch_num}_${clean_test_name_for_file}.ansi"
    local pass_marker="${current_output_dir}/batch_${batch_num}_${clean_test_name_for_file}.pass"
    local fail_marker="${current_output_dir}/batch_${batch_num}_${clean_test_name_for_file}.fail"

    rm -f "$pass_marker" "$fail_marker"

    echo -e "Batch $batch_num (Test: $clean_test_name_for_file) started at $(date +%Y%m%d_%H%M%S)" > "$batch_output_file"

    if ! go test -v -race -count=1 -run "$test_function_to_run" >> "$batch_output_file" 2>&1; then
            echo -e "\nBatch $batch_num (Test: $clean_test_name_for_file) FAILED at $(date +%Y%m%d_%H%M%S)" >> "$batch_output_file"
            touch "$fail_marker"
            return 1
    fi

    echo -e "\nBatch $batch_num (Test: $clean_test_name_for_file) FINISHED successfully at $(date +%Y%m%d_%H%M%S)" >> "$batch_output_file"
    echo -e "\n********************************************************************************" >> "$batch_output_file"
    echo -e "\n" >> "$batch_output_file"
    touch "$pass_marker"
}

export -f run_test_batch

cleanup_on_interrupt() {
    echo -e "\nScript interrupted by user."
    echo "Partially completed results (if any) are in: $output_dir_base"
    # 根据需要，可以在此处添加基于 KEEP_ONLY_FAILED 的部分清理逻辑
    # 但通常中断后保留所有已生成文件更安全，方便检查中断原因
    exit 1
}
trap cleanup_on_interrupt INT SIGINT SIGTERM

echo "Starting $TOTAL_TESTS tests for $TEST_PATTERN in parallel (Jobs: $PARALLEL_JOBS)..."
echo "Output will be in: $output_dir_base"
if [ "$KEEP_ONLY_FAILED" = true ]; then
    echo "KEEP_ONLY_FAILED is enabled: .ansi logs for passed tests will be removed after completion."
fi

parallel --bar --jobs $PARALLEL_JOBS \
    run_test_batch {#} "$output_dir_base" "$TEST_PATTERN" ::: $(seq 1 "$TOTAL_TESTS")

echo "All parallel test executions completed."

# 统计成功和失败的测试 (在删除任何日志之前进行)
total_initiated=$(find "$output_dir_base" \( -name "*.pass" -o -name "*.fail" \) -type f | wc -l | awk '{$1=$1};1') # awk to trim whitespace
passed_count=$(find "$output_dir_base" -name "*.pass" -type f | wc -l | awk '{$1=$1};1')
failed_count=$(find "$output_dir_base" -name "*.fail" -type f | wc -l | awk '{$1=$1};1')

# 如果 KEEP_ONLY_FAILED 为 true，则删除成功的测试的 .ansi 日志文件
if [ "$KEEP_ONLY_FAILED" = true ]; then
    echo "KEEP_ONLY_FAILED is true. Removing .ansi logs for passed tests..."
    cleaned_count=0
    find "$output_dir_base" -name "*.pass" -type f | while read -r pass_file; do
        ansi_file="${pass_file%.pass}.ansi"
        if [ -f "$ansi_file" ]; then
            rm "$ansi_file"
            cleaned_count=$((cleaned_count + 1))
        fi
        # 保留 .pass 文件用于统计，仅删除 .ansi 文件
    done
    if [ "$cleaned_count" -gt 0 ]; then
        echo "$cleaned_count .ansi log files for passed tests were removed."
    else
        echo "No .ansi log files for passed tests found to remove (or all failed/were not run to completion with a .pass file)."
    fi
fi

# 打印最终的测试总结
echo "--------------------------------------------------------------------------------"
echo "Test Execution Summary for: $test_name_for_dir"
echo "Total tests scheduled: $TOTAL_TESTS"
echo "Total test batches with .pass or .fail markers: $total_initiated"
echo "Passed: $passed_count"
echo "Failed: $failed_count"
echo "Full results (or only failed .ansi logs if KEEP_ONLY_FAILED=true) are in: $output_dir_base"
echo "--------------------------------------------------------------------------------"

if [ "$failed_count" -gt 0 ]; then
    echo "FAILURES DETECTED!"
    echo "Failed test log files:"
    # 查找所有 .fail 标记文件，并打印对应的 .ansi 文件名
    find "$output_dir_base" -name "*.fail" -type f | while read -r fail_file; do
        ansi_file_for_failed="${fail_file%.fail}.ansi"
        if [ -f "$ansi_file_for_failed" ]; then # 对应的 .ansi 文件应该总是存在
            echo "$ansi_file_for_failed"
        else
             # 理论上不应该发生，因为 .fail 意味着 .ansi 文件被创建了
            echo "Warning: Corresponding .ansi log not found for failed marker $fail_file"
        fi
    done
    echo "--------------------------------------------------------------------------------"
    echo "Please review the failed test logs listed above."
    exit 1
else
    # 检查是否所有预定的测试都已通过（并且都被启动了）
    if [ "$passed_count" -eq "$TOTAL_TESTS" ] && [ "$total_initiated" -eq "$TOTAL_TESTS" ]; then
        echo "ALL $TOTAL_TESTS TESTS PASSED SUCCESSFULLY!"
    elif [ "$passed_count" -eq "$total_initiated" ] && [ "$total_initiated" -lt "$TOTAL_TESTS" ] && [ "$total_initiated" -gt 0 ]; then
        echo "$passed_count (out of $total_initiated initiated) TESTS PASSED SUCCESSFULLY, but not all $TOTAL_TESTS batches were scheduled/run (possibly due to interruption before all jobs started)."
    elif [ "$total_initiated" -eq 0 ] && [ "$TOTAL_TESTS" -gt 0 ]; then
        echo "NO TESTS WERE INITIATED. Please check the script or parallel command."
    elif [ "$passed_count" -eq "$total_initiated" ] && [ "$total_initiated" -eq "$TOTAL_TESTS" ]; then # Redundant with first success case but good fallback
        echo "ALL INITIATED TESTS ($passed_count) PASSED SUCCESSFULLY!"
    else
        echo "Test summary: Some tests might not have completed as expected or only partial runs. Passed: $passed_count / Initiated: $total_initiated / Scheduled: $TOTAL_TESTS"
    fi
    exit 0
fi