import re
import sys

def extract_lines_with_color(input_file, target_color, output_file):
    """
    从指定路径的 .ansi 文件中提取带有指定 ANSI 颜色代码的行，并保存到新的 .ansi 文件中，保留颜色代码。
    
    参数：
    - input_file: 输入日志文件路径，例如 "/home/zks/mit6.824/lab3/src/kvraft/kvraft20250407_200803.ansi"
    - target_color: 目标颜色代码，例如 "[38;2;135;206;235m"
    - output_file: 输出文件路径，例如 "output.ansi"
    """
    # 清理颜色代码，移除方括号
    color_code = target_color.strip("[]")
    
    # 构造正则表达式，匹配指定的 ANSI 颜色代码及其整行内容
    # 匹配整行：从颜色代码开始到重置代码或行尾
    color_pattern = re.compile(r'\033\[' + re.escape(color_code) + r'(.*?)(?:\033\[0m|$|\n)', re.DOTALL)

    try:
        # 读取输入文件
        with open(input_file, 'r', encoding='utf-8') as f:
            log_content = f.read()

        # 调试：打印文件内容（所有行）
        print("Debug: File content:")
        lines = log_content.splitlines()
        for i, line in enumerate(lines, 1):
            print(f"Line {i}: {repr(line)}")

        # 查找所有匹配的行
        matches = color_pattern.finditer(log_content)
        extracted_lines = []
        for match in matches:
            line = match.group(0)
            # 确保每行以换行符结尾
            if not line.endswith('\n'):
                line += '\n'
            # 如果没有重置代码，添加一个
            if not line.endswith('\033[0m\n'):
                line = line.rstrip('\n') + '\033[0m\n'
            extracted_lines.append(line)

        if not extracted_lines:
            print(f"No lines found with color code: {target_color}")
            return

        # 将提取的行写入输出文件
        with open(output_file, 'w', encoding='utf-8') as f:
            f.writelines(extracted_lines)

        print(f"Extracted {len(extracted_lines)} lines with color code {target_color} to {output_file}")

    except FileNotFoundError:
        print(f"Error: Input file '{input_file}' not found.")
    except Exception as e:
        print(f"Error: {e}")

def main():
    # 检查命令行参数
    if len(sys.argv) != 4:
        print("Usage: python extract_color_lines.py <input_file> <color_code> <output_file>")
        print("Example: python extract_color_lines.py /home/zks/mit6.824/lab3/src/kvraft/kvraft20250407_200803.ansi '[38;2;135;206;235m' output.ansi")
        sys.exit(1)

    input_file = sys.argv[1]
    target_color = sys.argv[2]
    output_file = sys.argv[3]

    # 提取指定颜色的行并保存
    extract_lines_with_color(input_file, target_color, output_file)

if __name__ == "__main__":
    main()
