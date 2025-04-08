import sys
import os

def extract_lines_from_ansi_file(file_path, start_line, end_line, output_file):
    """
    从ANSI编码文件中提取指定行范围并保存为ANSI编码文件。
    
    参数:
        file_path (str): 输入文件路径
        start_line (int): 起始行号（从1开始计数）
        end_line (int): 结束行号（包含该行）
        output_file (str): 输出文件路径
    
    返回:
        bool: 操作是否成功
    """
    try:
        # 以ANSI编码（windows-1252）读取文件
        with open(file_path, 'r', encoding='windows-1252') as file:
            lines = file.readlines()
            
            # 验证行号范围是否有效
            if start_line < 1 or end_line > len(lines) or start_line > end_line:
                raise ValueError(f"行号范围无效：文件共 {len(lines)} 行，输入范围为 {start_line} 到 {end_line}。")
            
            # 提取指定行范围
            selected_lines = lines[start_line - 1:end_line]
        
        # 确保输出目录存在
        output_dir = os.path.dirname(output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # 保存为ANSI编码文件（windows-1252）
        with open(output_file, 'w', encoding='windows-1252') as file:
            file.writelines(selected_lines)
        
        print(f"已成功提取第 {start_line} 行到第 {end_line} 行，并保存至 '{output_file}'（ANSI编码）。")
        return True
    
    except FileNotFoundError:
        print(f"错误：文件 '{file_path}' 未找到。")
        return False
    except UnicodeDecodeError:
        print(f"错误：无法以ANSI编码（windows-1252）读取文件 '{file_path}'，请检查文件编码。")
        return False
    except UnicodeEncodeError:
        print(f"错误：无法以ANSI编码（windows-1252）保存文件，请检查内容是否包含不可编码字符。")
        return False
    except Exception as e:
        print(f"发生错误：{str(e)}")
        return False

def main():
    # 检查命令行参数
    if len(sys.argv) != 4:
        print("用法：python script.py <文件路径> <起始行号> <结束行号>")
        print("示例：python script.py example.txt 3 7")
        sys.exit(1)
    
    # 获取命令行参数
    file_path = sys.argv[1]
    try:
        start_line = int(sys.argv[2])
        end_line = int(sys.argv[3])
    except ValueError:
        print("错误：起始行号和结束行号必须为整数。")
        sys.exit(1)
    
    # 获取输入文件所在目录并生成输出文件名
    input_dir = os.path.dirname(file_path)
    base_name = os.path.basename(file_path).replace('.ansi', '_selected.ansi')
    output_file = os.path.join(input_dir, base_name)
    
    # 执行提取操作
    extract_lines_from_ansi_file(file_path, start_line, end_line, output_file)

if __name__ == "__main__":
    main()
