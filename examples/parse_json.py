import json

#/home/krb09/wlms2/wlms-emulator/examples/het_resources/l2f
filename_json = 'profile.executor.2020.04.09.10.46.15.031055.0000.json'

with open(filename_json, 'r') as f:
    filename_dict = json.load(f)

list_dict=filename_dict[u'']
x = list(map(lambda x: x["exec_time"], list_dict))

file_write_name="test1.txt"
file_write=open(file_write_name,"a") 
for i in x:
    file_write.write(str(i))
    file_write.write("\n")
file_write.close()

