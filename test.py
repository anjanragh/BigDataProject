import subprocess

dir_in = "/user/hm74/NYCOpenData/"
args = "hdfs dfs -ls "+dir_in+" | awk '{print $8}'"

proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

s_output, s_err = proc.communicate()
all_dart_dirs = s_output.split()

filesToRead = []

for f in all_dart_dirs:
   filesToRead.append(f.decode("utf-8"))
