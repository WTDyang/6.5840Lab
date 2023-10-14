import os
with open("errors.log","w") as w:
    # 遍历所有文件，找到包含“--- FAIL”的日志
    for i in range(1, 2001):
        filename = f"test-{i}.log"  # 构造文件名
        if not os.path.isfile(filename):  # 判断当前文件是否存在
            continue
        with open(filename, "r") as f:  # 打开日志文件
            lines = f.readlines()  # 读取全部内容
            for line_no, line in enumerate(lines):  # 使用enumerate遍历日志文件中的每一行，并记下行号
                if "--- FAIL" in line:  # 如果该行包含“--- FAIL”
                    w.write(f"in {filename} : {line_no + 1} find error:{line}")  # 输出行号和日志内容
                if "config.go" in line:  # 如果该行包含“config.go”
                    w.write(f"{filename}:{line_no + 1}:{line}")