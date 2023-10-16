import os
with open("errors.log","w") as w:
    for i in range(1, 2001):
        filename = "test-%d.log" %i
        if not os.path.isfile(filename):  
            continue
        with open(filename, "r") as f:  
            lines = f.readlines()  
            for line_no, line in enumerate(lines): 
                if "--- FAIL" in line:
                    w.write("in %s : %d find error: %s" %(filename,line_no+1,line))
                if "config.go" in line:
                    w.write("%s:%d: %s" %(filename,line_no+1,line))