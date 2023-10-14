import os
with open("errors.log","w") as w:
    for i in range(1, 2001):
        filename = f"test-{i}.log"  
        if not os.path.isfile(filename):  
            continue
        with open(filename, "r") as f:  
            lines = f.readlines()  
            for line_no, line in enumerate(lines): 
                if "--- FAIL" in line: 
                    w.write(f"in {filename} : {line_no + 1} find error:{line}")  
                if "config.go" in line: 
                    w.write(f"{filename}:{line_no + 1}:{line}")