import json
import sys

#/home/krb09/wlms2/wlms-emulator/examples/het_resources/l2f
filename_profile = str(sys.argv[1])
filename_json = filename_profile

with open(filename_json, 'r') as f:
    filename_dict = json.load(f)

print(filename_dict.keys())
list_of_keys = filename_dict.keys()

print(list_of_keys[0])

key_string = "u'" + str(list_of_keys[0]) + "'"

list_dict=filename_dict[list_of_keys[0]]
x = list(map(lambda x: x["exec_time"], list_dict))

file_write_name="test1.txt"
file_write=open(file_write_name,"a") 
for i in x:
    file_write.write(str(i))
    file_write.write("\n")
file_write.close()

