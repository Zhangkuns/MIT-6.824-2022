#!/bin/bash


# 设置测试总次数（默认为200，可以通过命令行参数修改）
TOTAL_TESTS=${1:-200}  # 如果没有提供参数，默认为200
PARALLEL_JOBS=35      # 固定并行任务数为35

# 获取当前目录
current_dir=$(pwd)

# 创建一个时间戳目录来存储所有输出 (在当前目录下)
timestamp=$(date +%Y%m%d_%H%M%S)
output_dir="${current_dir}/${timestamp}_test2C_results"

# 如果创建目录失败，则退出
if ! mkdir -p "$output_dir"; then
    echo "Error: Cannot create output directory '$output_dir'" >&2
    exit 1
fi

# 创建运行测试的函数
run_test_batch() {
    local batch_num=$1
    local output_dir=$2  # 显式传递output_dir参数
    local batch_output="${output_dir}/batch_${batch_num}.ansi"

    # 使用单个echo命令并确保文件存在
    echo -e "Batch $batch_num started" > "$batch_output"

    # 追加测试输出
    if ! go test -run 2C >> "$batch_output" 2>&1; then
            echo -e "\nBatch $batch_num failed" >> "$batch_output"
            return 1
    fi

    # shellcheck disable=SC2129
    echo -e "\nBatch $batch_num finished" >> "$batch_output"
    echo -e "\n********************************************************************************" >> "$batch_output"
    echo -e "\n" >> "$batch_output"
}

export -f run_test_batch
export output_dir  # 导出环境变量

# 处理脚本中断 (例如 Ctrl+C)
trap 'rm -rf "$output_dir"; exit 1' INT

# 并行运行测试
seq 1 "$TOTAL_TESTS" | parallel --bar --jobs=$PARALLEL_JOBS run_test_batch {} "$output_dir"

# 修改合并部分的代码
echo "Merging test results..."
# 使用 ls 和 sort 来确保按数字顺序合并文件
# shellcheck disable=SC2012
ls "${output_dir}"/batch_*.ansi | sort -V | xargs cat > "${output_dir}/${timestamp}_combined.ansi"

echo "All tests completed. Results saved in ${output_dir}/${timestamp}_combined.ansi"