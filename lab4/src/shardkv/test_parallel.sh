#!/bin/bash

# 参数1: 测试总批次数 (默认为3)
TOTAL_BATCH_RUNS=${1:-3} # 改为总批次数

# 参数2: 用于发现测试函数的Go正则表达式 (默认为 "Test" 匹配所有以Test开头的函数)
USER_PROVIDED_PATTERN="${2:-Test}"

# 参数3: 是否只保留失败的日志
KEEP_ONLY_FAILED_INPUT="${3:-false}"
KEEP_ONLY_FAILED=false
if [[ "$KEEP_ONLY_FAILED_INPUT" == "true" || "$KEEP_ONLY_FAILED_INPUT" == "TRUE" || "$KEEP_ONLY_FAILED_INPUT" == "1" ]]; then
    KEEP_ONLY_FAILED=true
fi

PARALLEL_JOBS=${PARALLEL_JOBS:-32}

current_dir=$(pwd)
timestamp=$(date +%Y%m%d_%H%M%S)

if [[ "$USER_PROVIDED_PATTERN" == "^"* ]]; then
    GO_TEST_LIST_PATTERN="$USER_PROVIDED_PATTERN"
else
    GO_TEST_LIST_PATTERN="^${USER_PROVIDED_PATTERN}"
fi

test_suite_name_for_dir=$(echo "$USER_PROVIDED_PATTERN" | sed -e 's/\^//g' -e 's/\$//g' -e 's/\.\*//g' -e 's/\.$//g')
if [[ "$GO_TEST_LIST_PATTERN" == "^Test" ]]; then
    test_suite_name_for_dir="AllTests"
fi

# 顶层输出目录
top_output_dir="${current_dir}/${timestamp}_${test_suite_name_for_dir}_totalBatches${TOTAL_BATCH_RUNS}"

if ! mkdir -p "$top_output_dir"; then
    echo "Error: Cannot create top output directory '$top_output_dir'" >&2
    exit 1
fi

# 创建运行单个测试的函数 (在一个特定批次的目录内)
# 参数: 1:测试函数名, 2:当前批次的输出目录
run_test_in_batch_dir() {
    local test_function_name=$1
    local batch_specific_output_dir=$2 # 例如 top_output_dir/batch_1

#    echo "DEBUG_FUNC_CALL: test_function_name='${test_function_name}', batch_specific_output_dir='${batch_specific_output_dir}'"

    # 从 batch_specific_output_dir 提取批次名称，例如 "batch_1"
    local batch_dir_name=$(basename "$batch_specific_output_dir")
    # 从批次名称中提取数字，例如从 "batch_1" 提取 "1"
    local batch_num_for_display=${batch_dir_name#batch_} # 移除 "batch_" 前缀

#    echo "DEBUG_FUNC_PARSE: batch_dir_name='${batch_dir_name}', batch_num_for_display='${batch_num_for_display}'"

    local exact_test_run_pattern="^${test_function_name}$"
    local output_file_basename="${batch_specific_output_dir}/${test_function_name}" # 文件名不含run_num
    local log_file="${output_file_basename}.ansi"
    local pass_marker="${output_file_basename}.pass"
    local fail_marker="${output_file_basename}.fail"

    rm -f "$pass_marker" "$fail_marker"

    echo -e "Running ${test_function_name} in Batch${batch_num_for_display}"
    {
        echo -e "Test: ${test_function_name}"
        echo -e "Batch Directory: ${batch_specific_output_dir}"
        echo -e "Started at: $(date +%Y%m%d_%H%M%S)"
        echo -e "Command: go test -v -race -count=1 -run \"${exact_test_run_pattern}\"\n"
    } > "$log_file"

    if ! go test -v -race -count=1 -run "$exact_test_run_pattern" >> "$log_file" 2>&1; then
        echo -e "\nTest: ${test_function_name} FAILED in ${batch_specific_output_dir} at $(date +%Y%m%d_%H%M%S)" >> "$log_file"
        touch "$fail_marker"
        echo -e "=> FAILED: ${test_function_name} (in $(basename "$batch_specific_output_dir"))"
        return 1
    fi

    echo -e "\nTest: ${test_function_name} FINISHED successfully in ${batch_specific_output_dir} at $(date +%Y%m%d_%H%M%S)" >> "$log_file"
    echo -e "\n********************************************************************************\n" >> "$log_file"
    touch "$pass_marker"
    echo -e "=> PASSED: ${test_function_name} (in $(basename "$batch_specific_output_dir"))"
    return 0
}
export -f run_test_in_batch_dir

cleanup_on_interrupt() {
    echo -e "\nScript interrupted by user."
    echo "Partially completed results (if any) are in: $top_output_dir"
    exit 130
}
trap cleanup_on_interrupt INT SIGINT SIGTERM

echo "--------------------------------------------------------------------------------"
echo "DEBUG SCRIPT: Starting test discovery..."
echo "DEBUG SCRIPT: User provided pattern for discovery: '$USER_PROVIDED_PATTERN'"
echo "DEBUG SCRIPT: Effective pattern for 'go test -list': '$GO_TEST_LIST_PATTERN'"
discovered_functions_output_raw=$(go test -list "$GO_TEST_LIST_PATTERN" 2>/dev/null)
grep_pattern_for_test_names="^Test"
if [[ ! "$USER_PROVIDED_PATTERN" == "Test" && ! "$USER_PROVIDED_PATTERN" == "" ]]; then
    temp_grep_pattern=$(echo "$USER_PROVIDED_PATTERN" | sed 's/^\^//')
    grep_pattern_for_test_names="^${temp_grep_pattern}"
fi
discovered_functions=$(echo "$discovered_functions_output_raw" | grep "$grep_pattern_for_test_names" | awk '{print $1}' | sort -u)

if [ -z "$discovered_functions" ]; then
    echo "Error: No test functions found matching list pattern '$GO_TEST_LIST_PATTERN' after filtering."
    exit 1
fi
echo -e "Found the following test functions to run:\n$discovered_functions"
echo "Each function will be run once per batch, for a total of $TOTAL_BATCH_RUNS batches."
echo "--------------------------------------------------------------------------------"

total_test_instances_across_all_batches=0

# 外层循环，控制批次数
for batch_idx in $(seq 1 "$TOTAL_BATCH_RUNS"); do
    current_batch_dir="${top_output_dir}/batch_${batch_idx}"
    if ! mkdir -p "$current_batch_dir"; then
        echo "Error: Cannot create batch directory '$current_batch_dir'" >&2
        continue # 跳过这个批次
    fi

#    echo "================================================================================"
#    echo "Starting Batch $batch_idx / $TOTAL_BATCH_RUNS"
#    echo "Output for this batch will be in: $current_batch_dir"
#    echo "DEBUG_MAIN_LOOP: Current batch_idx = $batch_idx, current_batch_dir = $current_batch_dir" # <--- 新增
#    echo "--------------------------------------------------------------------------------"

    declare -a tasks_for_this_batch
    tasks_for_this_batch=()
    while IFS= read -r func_name; do
        if [[ -n "$func_name" ]]; then
            tasks_for_this_batch+=("$func_name $current_batch_dir")
            total_test_instances_across_all_batches=$((total_test_instances_across_all_batches + 1))
        fi
    done <<< "$discovered_functions"

#     # ---- 调试输出 tasks_for_this_batch ----
#        echo "DEBUG_MAIN_LOOP: Tasks generated for batch_idx $batch_idx:"
#        printf "    '%s'\n" "${tasks_for_this_batch[@]}"
#        echo "--------------------------------------"
#    # ---- 调试输出结束 ----

    if [ ${#tasks_for_this_batch[@]} -eq 0 ]; then
        echo "Warning: No tasks generated for batch $batch_idx."
        continue
    fi

    (
      for task_line in "${tasks_for_this_batch[@]}"; do
        echo "$task_line"
      done
    ) | parallel --bar --jobs "$PARALLEL_JOBS" --colsep ' ' \
        run_test_in_batch_dir {1} {2}

    echo "--------------------------------------------------------------------------------"
    echo "Batch $batch_idx / $TOTAL_BATCH_RUNS completed."
    echo "================================================================================"
    echo # 空行
done


echo "All test batches completed."
echo "--------------------------------------------------------------------------------"

# 统计成功和失败的测试
total_initiated=$(find "$top_output_dir" \( -name "*.pass" -o -name "*.fail" \) -type f | wc -l | awk '{$1=$1};1')
passed_count=$(find "$top_output_dir" -name "*.pass" -type f | wc -l | awk '{$1=$1};1')
failed_count=$(find "$top_output_dir" -name "*.fail" -type f | wc -l | awk '{$1=$1};1')

# 如果 KEEP_ONLY_FAILED 为 true，则删除成功的测试的 .ansi 日志文件
if [ "$KEEP_ONLY_FAILED" = true ]; then
    echo "KEEP_ONLY_FAILED is true. Removing .ansi logs for passed tests..."
    cleaned_count=0
    find "$top_output_dir" -name "*.pass" -type f | while read -r pass_file; do
        ansi_file="${pass_file%.pass}.ansi"
        if [ -f "$ansi_file" ]; then
            rm "$ansi_file"
            cleaned_count=$((cleaned_count + 1))
        fi
    done
    if [ "$cleaned_count" -gt 0 ]; then
        echo "$cleaned_count .ansi log files for passed tests were removed."
    else
        echo "No .ansi log files for passed tests found to remove."
    fi
fi

# 打印最终的测试总结
echo "--------------------------------------------------------------------------------"
echo "Overall Test Execution Summary for pattern: '$USER_PROVIDED_PATTERN'"
echo "Total batches scheduled: $TOTAL_BATCH_RUNS"
echo "Total individual test executions scheduled across all batches: $total_test_instances_across_all_batches"
echo "Total test executions with .pass or .fail markers: $total_initiated"
echo "Passed: $passed_count"
echo "Failed: $failed_count"
echo "Full results (or only failed .ansi logs if KEEP_ONLY_FAILED=true) are in: $top_output_dir"
echo "--------------------------------------------------------------------------------"

if [ "$failed_count" -gt 0 ]; then
    echo "FAILURES DETECTED!"
    echo "Failed test log files (check within respective batch subdirectories):"
    find "$top_output_dir" -name "*.fail" -type f | while read -r fail_file; do
        ansi_file_for_failed="${fail_file%.fail}.ansi"
        if [ -f "$ansi_file_for_failed" ]; then
            echo "$ansi_file_for_failed"
        else
            echo "Warning: Corresponding .ansi log not found for failed marker $fail_file"
        fi
    done
    echo "--------------------------------------------------------------------------------"
    echo "Please review the failed test logs listed above."
    exit 1
else
    # 这里的成功条件也需要相应调整
    if [ "$passed_count" -eq "$total_test_instances_across_all_batches" ] && [ "$total_initiated" -eq "$total_test_instances_across_all_batches" ]; then
        echo "ALL $total_test_instances_across_all_batches TEST EXECUTIONS ACROSS ALL BATCHES PASSED SUCCESSFULLY!"
    else
        echo "Test summary: Passed: $passed_count / Initiated (marked): $total_initiated / Scheduled: $total_test_instances_across_all_batches. Some tests might not have completed as expected or there were discrepancies."
    fi
    exit 0
fi