#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import re
import concurrent.futures
import asyncio
import time
from random import randint
import subprocess

cdr_record_length = 77

@asyncio.coroutine
def handle_connection (reader, writer):
	while True:
		try:
			data = yield from asyncio.wait_for(reader.readline(), timeout=10.0)
			if data:
				try:
					fh = open("/var/log/cdr/cdr.raw", "ab")
					fh.write(data)
				except IOError as msg:
					sys.stderr.write("[ERROR] {0}: {1}\n".format(msg.errno, msg.strerror))
				except:
					sys.stderr.write("[ERROR] Unexpected: {0}\n".format(sys.exc_info()[0]))
				finally:
					fh.close()
				data = data.decode("ascii").replace("\n","").replace("\r","").replace("\x00","")
				data = re.sub(r"\d{2}:\d{2} \d{2}/\d{2}","",data)
				print(data)
				if len(data) >= cdr_record_length:
					day = data[0:2].replace(" ","")
					month = data[2:4].replace(" ","")
					year = data[4:6].replace(" ","")
					hours = data[7:9].replace(" ","")
					minutes = data[9:11].replace(" ","")
					sec_dur = data[12:17].replace(" ","")
					dialed_num = data[18:39].replace(" ","")
					dialed_num = dialed_num.replace("#","")
					calling_num = data[40:55].replace(" ","")
					code_used = data[56:60].replace(" ","")
					code_dial = data[61:65].replace(" ","")
					in_trk_code = data[66:70].replace(" ","")
					frl = data[71].replace(" ","")
					out_crt_id = data[73:77].replace(" ","")
					cdr_csv_record = day+';'+month+';'+year+';'+hours+';'+minutes+';'+sec_dur.strip("0")+';'+dialed_num+';'+calling_num+';'+code_used+';'+code_dial+';'+in_trk_code+';'+frl+';'+out_crt_id+"\n"
					cdr_utm5_record = calling_num+';'+dialed_num+';'+sec_dur.strip("0")+';'+day+'-'+month+'-'+year+' '+hours+':'+minutes+':'+"00\n"
					try:
						fh = open("/var/log/cdr/cdr.csv",  "a")
						fh.write(cdr_csv_record)
					except IOError as msg:
						sys.stderr.write("[ERROR] {0}: {1}\n".format(msg.errno, msg.strerror))
					except:
						sys.stderr.write("[ERROR] Unexpected: {0}\n".format(sys.exc_info()[0]))
					finally:
						fh.close()
					timestamp = int(time.time())
					data_filename = "/var/log/cdr/"+str(timestamp) + str(randint(0, timestamp))
					cmd = ""
					try:
						fh = open(data_filename, "w")
						fh.write(cdr_utm5_record)
						cmd = "/netup/utm5/bin/utm5_send_cdr -c /netup/utm5/utm5_send_cdr.cfg -s " + data_filename
						print(cmdname + "\n")
#						os.system(cmd)
#						subprocess.run(["/netup/utm5/bin/utm5_send_cdr", "-c", "/netup/utm5/utm5_send_cdr.cfg", "-s", data_filename])
					except IOError as msg:
						sys.stderr.write("[ERROR] {0}: {1}\n".format(msg.errno, msg.strerror))
					except:
						sys.stderr.write("[ERROR] Unexpected: {0}\n".format(sys.exc_info()[0]))
					finally:
						fh.close()
					os.system(cmd)
#					os.remove(data_filename)

			else:
				break

		except concurrent.futures.TimeoutError:
			break

	writer.close()

server_ip = "10.1.2.4"
server_port = 5005

loop = asyncio.get_event_loop()
server_generator = asyncio.start_server(handle_connection, server_ip, server_port, loop=loop)
server = loop.run_until_complete(server_generator)

try:
	loop.run_forever()
except KeyboardInterrupt:
	pass
finally:
	server.close()
	loop.run_until_complete(server.wait_closed())
	loop.close()
