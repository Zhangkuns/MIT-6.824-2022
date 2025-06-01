import re
import sys

def extract_lines_with_color(input_file, target_color, output_file):
    """
    从 .ansi 文件中提取带有指定 ANSI 颜色代码的行，保留完整格式。
    
    参数：
    - input_file: 输入日志文件路径
    - target_color: 目标颜色代码，例如 "[38;2;135;206;235m"
    - output_file: 输出文件路径
    """
    # 清理颜色代码，移除方括号
    color_code = target_color.strip("[]")
    
    # 正则表达式匹配时间戳开头，指定颜色代码，直到重置代码或行尾
    color_pattern = re.compile(
        r'(\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})\s+'  # 时间戳
        r'\033\[' + re.escape(color_code) + r'([^\033]+?)(?=\033\[0m|\n|$)',  # 颜色内容，非贪婪匹配
        re.MULTILINE
    )

    try:
        # 读取输入文件
        with open(input_file, 'r', encoding='utf-8') as f:
            log_content = f.read()

        # 查找所有匹配的行
        matches = color_pattern.findall(log_content)
        if not matches:
            print(f"No lines found with color code: {target_color}")
            return

        extracted_lines = []
        for timestamp, content in matches:
            # 构造完整的行，保留 ANSI 颜色代码
            line = f"{timestamp} \033[{color_code}{content}\033[0m\n"
            extracted_lines.append(line)

        # 写入输出文件
        with open(output_file, 'w', encoding='utf-8') as f:
            f.writelines(extracted_lines)

        print(f"Extracted {len(extracted_lines)} lines with color code {target_color} to {output_file}")

    except FileNotFoundError:
        print(f"Error: Input file '{input_file}' not found.")
    except Exception as e:
        print(f"Error: {e}")

def main():
    if len(sys.argv) != 4:
        print("Usage: python extract_color_lines.py <input_file> <color_code> <output_file>")
        print("Example: python extract_color_lines.py input.ansi '[38;2;135;206;235m' output.ansi")
        sys.exit(1)

    input_file = sys.argv[1]
    target_color = sys.argv[2]
    output_file = sys.argv[3]

    extract_lines_with_color(input_file, target_color, output_file)

if __name__ == "__main__":
    main()