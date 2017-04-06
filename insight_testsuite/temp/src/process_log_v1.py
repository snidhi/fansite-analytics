# // your Python code to implement the features could be placed here
# // note that you may use any language, there is no preference towards Python

# // load file
# // readlines
# // split, strip

#!/usr/bin/python -tt
import sys
# #import numpy as np
# import matplotlib.pyplot as plt
# from sklearn import linear_model, datasets
# import time
import re
import datetime
# import nltk


def main():

# Read the input file 

             filename = sys.argv[1]
             f = open(filename, 'r')
             log = f.readlines()
             
             filename = sys.argv[2]
             f1 = open(filename, 'w')
             
             filename = sys.argv[4]
             f2 = open(filename, 'w')             
             
             filename = sys.argv[3]
             f3 = open(filename, 'w')
             
             filename = sys.argv[5]
             f4 = open(filename, 'w')             
             
             hashmap_hostname = {}
             hashmap_resources = {}
             hashmap_hr_freq = {}

             line = log[0]
             hostname = line.split(' - - ')[0]
             restofstring = line.split(' - - ')[1]                 
             restofstring = restofstring.split('"')
             old_timestamp = restofstring[0].strip()
             date = convert_time(old_timestamp)
             start = datetime.datetime( date[0],date[1],date[2],date[3],date[4],date[5])
             abs_start = start
             timestamp_list = []
             frequency = []
             timestamp_freq = []
             count = 0
             hour_freq =0
             sec_freq = 0
             sec_start = start
             time_window = 3600
             failed_attempts = 0
             blocked = False
             failed_hostname = ''

             
             line = log[-1]

             hostname = line.split(' - - ')[0]
             restofstring = line.split(' - - ')[1]                 
             restofstring = restofstring.split('"')
             current_time_timestamp = restofstring[0].strip()
             date = convert_time(current_time_timestamp)
             abs_current_time = datetime.datetime( date[0],date[1],date[2],date[3],date[4],date[5])
             start_index = 0
             i = 0        
             
             failed_hostname = []
             failed_attempts = {}
             blocked= {}
             failed_attempts = {}
             t_blocked = {}
             t = {}
             cnt = 0 

             for line in log:
		 cnt+=1
		 if (cnt % 10000 == 0):
			 print cnt
		 try:
		 	ondash = line.split(' - - ')
                 	hostname = ondash[0]
                 	restofstring = ondash[1]                 
                 	restofstring = restofstring.split('"')
                 	timestamp = restofstring[0].strip()
                 	inside_commas = restofstring[1].split()
			if len(inside_commas) == 1:
				resource = inside_commas[0]
			else:
				resource = inside_commas[1]
		 	code_and_bytes = restofstring[-1].split()
                 	http_reply_code = code_and_bytes[0].strip()
		 	bytes1 = 0		   
		 	if code_and_bytes[-1] != '-':
				bytes1 = int(code_and_bytes[-1].strip())

		 except:
			print line
			print("Unexpected error:", sys.exc_info()[0])
# feature 1   
                 
                 if hostname in hashmap_hostname:
                    freq = hashmap_hostname[hostname] + 1
                    
                 else:
                    freq = 1
                    
                 hashmap_hostname[hostname] = freq
                 
# feature 2

                 if resource in hashmap_resources:
                     
                    bandwidth = hashmap_resources[resource] + bytes1
                    
                 else:
                    bandwidth = bytes1
                    
                 hashmap_resources[resource] = bandwidth

                 date = convert_time(timestamp)
                 current_time = datetime.datetime(date[0],date[1],date[2],date[3],date[4],date[5])
# feature 4


                 if(http_reply_code == '401' and hostname not in failed_hostname):                    
                    t[hostname] = current_time
                    failed_attempts[hostname] = 1
                    failed_hostname.append(hostname)
                 
                 if(http_reply_code == '401' and hostname in failed_hostname):
                    if (current_time-t.get(hostname)).total_seconds() <= 20.0 and (current_time-t.get(hostname)).total_seconds() > 0.0:
                                       
                         failed_attempts[hostname] = failed_attempts.get(hostname)+1
                    else:
                         t[hostname] = current_time
                         failed_attempts[hostname] = 1
                         
                 if failed_attempts.get(hostname) == 3:
                     
                     blocked[hostname] = True
                     t_blocked[hostname] = current_time
                                
                 if(blocked.get(hostname) and (current_time - t_blocked.get(hostname)).total_seconds() <= 300 and (current_time - t_blocked.get(hostname)).total_seconds() > 0.0):                        
                        
                        f4.write(line + '\n')

# feature 3

                 diff = current_time - start
                 
                 sec_diff = current_time - sec_start
                 
                 if(sec_diff.total_seconds() == 0):
                    
                    sec_freq = sec_freq+1
                    
                 else:
                    timestamp_freq.append(sec_freq) 
                    sec_freq = 1
                    for i in range(int(sec_diff.total_seconds())-1):
                        timestamp_freq.append(0)
                    sec_start = sec_start + + datetime.timedelta(seconds=int(sec_diff.total_seconds()))
                    
                #  print "timestamp_freq", timestamp_freq
                #  print "diff.total_seconds()", diff.total_seconds()
                 
                     
                    
                #  if(diff.total_seconds() < time_window):                     
                                                        
                #      hour_freq = hour_freq+1
                    
                #  else:#(diff.total_seconds() ==  time_window):
                    
                #     hashmap_hr_freq[reverse_convert_time(str(start))] = hour_freq
                #     hour_freq = hour_freq - timestamp_freq[i] + 1
                #     start = start + + datetime.timedelta(seconds=1)
                #     old_timestamp = timestamp
                #     i = i+1
                #  else:
                     
                #      hashmap_hr_freq[reverse_convert_time(str(start))] = hour_freq
                #      date = convert_time(timestamp)
                #      start = datetime.datetime(date[0],date[1],date[2],date[3],date[4],date[5])
                #      start = current_time#start - - datetime.timedelta(seconds=time_window+1)
                #      hour_freq = 1
                #     #  hashmap_hr_freq[start] = hour_freq
                #      old_timestamp = timestamp
                     
                #  print "hour_freq",start, hour_freq    
                 hashmap_hr_freq[reverse_convert_time(str(start))] = hour_freq 
             timestamp_freq.append(sec_freq)
             
             hashmap_hr_freq[reverse_convert_time(str(current_time))] = sec_freq 
            
             j1 =0
             j2 = j1+time_window
             
             time = abs_start
             while j1 <= len(timestamp_freq):
                
                if j2 <= len(timestamp_freq):
                    for j in range(j1,j2):
                        hour_freq = hour_freq + timestamp_freq[j]
                     
                    j2 = j1+time_window
                else:
                    for j in range(j1,len(timestamp_freq)):
                        hour_freq = hour_freq + timestamp_freq[j]
                j1 = j1+1                
                hashmap_hr_freq[reverse_convert_time(str(time))] = hour_freq
                time = time + + datetime.timedelta(seconds=1)
                hour_freq =0
             
             
             if len(hashmap_hostname) < 11:
             
                k = sorted(hashmap_hostname, key=hashmap_hostname.get, reverse=True)
                
             else:    
                 
                 k = sorted(hashmap_hostname, key=hashmap_hostname.get, reverse=True)[:10]

             for word in k:
                 l = word+','+str(hashmap_hostname.get(word))+'\n'
                 f1.write(l)                 
             
             if len(hashmap_resources) < 11:
             
                k = sorted(hashmap_resources, key=hashmap_resources.get, reverse=True)
                
             else:    
                 
                 k = sorted(hashmap_resources, key=hashmap_resources.get, reverse=True)[:10]

             for word in k:
                 
                 f2.write(word+'\n')
                 
             if len(hashmap_hr_freq) < 11:
                s = sorted (hashmap_hr_freq.items(), key= lambda (k,v) :(-v,k))
                
             else:    
                 
                s = sorted (hashmap_hr_freq.items(), key= lambda (k,v) :(-v,k))[:10]
                 
             for word in s:
                
                f3.write(str(word)[2:28]+','+str(word)[31:-1] +'\n')#+','+str(hashmap_hr_freq.get(word))+'\n')
                 
   
                 
def convert_time(timestamp):
             months_map = {'Jan': 01, 'Feb' : 02, 'Mar':03, 'Apr':04, 'May':05, 'Jun':06, 'Jul':07, 'Aug':8, 'Sep':9, 'Oct':10, 'Nov':11, 'Dec':12   } 

             day = int(timestamp[1:3])
             month = (months_map.get(timestamp[4:7]))
             year = int(timestamp[8:12])
             hour = int(timestamp[13:15])
             minute = int(timestamp[16:18])
             second = int(timestamp[19:21])
             return(year, month, day, hour, minute, second)
             
def reverse_convert_time(start):
        
              reverse_months_map = {1 : 'Jan', 2:'Feb', 3:'Mar', 4:'Apr', 5:'May', 6:'Jun', 7:'Jul',8:'Aug', 9:'Sep', 10:'Oct', 11:'Nov', 12:'Dec'   } 
              year = start[0:4]
              month_name = reverse_months_map.get(int(start[5:7]))
              day = start[8:10]
              hour = start[11:13]
              minute = start[14:16]
              second = start[17:]

              timestamp = ''+day+'/'+month_name+'/'+year+':'+hour+':'+minute+':'+second+' -0400'

              return(timestamp)
                 
# Creting the main 
if __name__ == '__main__':
  main()                 
             
