#!/usr/bin/env python
# encoding: utf-8
import datetime

from cooler import Cooler


class ShortenCooler(Cooler):

    def settings(self):
        self.MONGO_HOST = "192.20.20.11"
        self.MONGO_PORT = 27017
        #self.MONGO_HOST = "172.100.102.163"
        #self.MONGO_PORT = 27019
        self.path = '/home/qfpay/shorten/log/shorten_1_access.log'
        #self.path = '/Users/weather/git/collog/bin/shorten_1_access.log'
        self.line_num = 0
        self.db = "bi"
        self.collection = "shorten_log"

    def get_info(self, line):
        note = {}
        try:
            wholelog = line.split(" - - ", 1)
            note["home_ip"] = wholelog[0]
            wholelog = wholelog[1].split(" \"", 1)
            note["time"] = datetime.datetime.strptime(wholelog[0].replace(" +0800", "")[1:-1], "%d/%b/%Y:%H:%M:%S")
            wholelog = wholelog[1].split(" \"-\" ")
            #info1
            info1 = wholelog[0].replace("\"", "").split(" ")
            note["method"] = info1[0]
            if "?" in info1[1]:
                note["url"] = info1[1].split("?")[0][1:]
                note["param"] = info1[1].split("?")[1]
            else:
                note["url"] = info1[1][1:]
            note["status1"] = info1[3]
            note["status2"] = info1[4]
            note["ua"] = wholelog[1][1:].split("\" \"")[0]
            note["ip"] = wholelog[1][1:].split("\" \"")[1].split(",")[0]
        except:
            pass
        return note

if __name__ == '__main__':
    cool = ShortenCooler()
    #print cool.get_info('10.12.12.51 - - [05/Jan/2016:15:28:07] "GET /490NcBP HTTP/1.0" 302 349 "-" "Mozilla/5.0 (Linux; U; Android 5.0.1; zh-cn; MX4 Build/LRX22C) AppleWebKit/533.1 (KHTML, like Gecko)Version/4.0 MQQBrowser/5.4 TBS/025489 Mobile Safari/533.1 MicroMessenger/6.3.8.50_r251a77a.680 NetType/3gnet Language/zh_CN" "112.97.51.190, 10.12.12.51"')
    #print cool.get_info('10.12.12.51 - - [08/Jul/2015:16:50:19] "GET /W8AMu6k HTTP/1.0" 302 471 "-" "Mozilla/5.0 (Linux; U; Android 4.3; zh-cn; GT-I9308 Build/JSS15J) AppleWebKit/533.1 (KHTML, like Gecko)Version/4.0 MQQBrowser/5.4 TBS/025411 Mobile Safari/533.1 MicroMessenger/6.2.0.54_r1169949.561 NetType/cmnet Language/zh_CN"')
    cool.run()
