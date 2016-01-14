#!/usr/bin/env python
# encoding: utf-8

import os
import subprocess
import time
import pymongo
import datetime
import json


class Cooler(object):

    def __init__(self):
        self.setAttr("192.20.20.11", 27017, '/home/qfpay/publish/near-api/log/near_note_0.log', 0, "bi", "note")
        self.settings()

    def settings(self):
        pass

    def setAttr(self, MONGO_HOST, MONGO_PORT, path, line_num, db, collection):
        self.MONGO_HOST = MONGO_HOST
        self.MONGO_PORT = MONGO_PORT
        self.path = path
        self.line_num = line_num
        self.db = db
        self.collection = collection

    def get_info(self, line):
        note = {}
        try:
            wholelog = line.split("[NOTE]")
            time = datetime.datetime.strptime(wholelog[0].split(",")[0], "%Y-%m-%d %H:%M:%S")
            infos = wholelog[1].split("|")
            try:
                args = json.loads(infos[9].strip())
            except:
                args = infos[9]
            if len(infos) <= 11:
                note = {
                    "user_id": infos[0].strip() if infos[0] != "null" else "",
                    "method": infos[1],
                    "path": infos[2],
                    "host": infos[3],
                    "status": infos[4],
                    "respcd": infos[5],
                    "remote_ip": infos[6],
                    "duration": infos[7],
                    "UA": infos[8],
                    "args": args,
                    "datetime": time
                }
                if note["path"] == "/locate_circle":
                    #地理位置
                    try:
                        data = json.loads(infos[10])["data"]
                        note.update({
                            "city_id": data["current_city"]["area_id"],
                            "city_region_id": data["in_biz_circle"]["id"]
                        })
                    except:
                        pass
                if note["path"] in ["/android_bind", "/ios_bind", "/wp_bind", "/crash_report", "/ping"]:
                    return {}
            else:
                note = {"log": line, "datetime": time}
        except:
            pass
        return note

    def importlog(self, filename):
        logfile = open(filename, 'r')
        while 1:
            line = logfile.readline()
            try:
                info = self.get_info(line)
                note = {}
                if info:
                    note.update(info)
                    db = pymongo.MongoClient(self.MONGO_HOST, self.MONGO_PORT)[self.db]
                    result = db[self.collection].insert_one(note)
            except Exception, e:
                print e
        logfile.close()

    def run(self):
        while 1:
            if not (self.path and os.path.exists(self.path)):
                print 'Invalid path: %s' % self.path
                time.sleep(1)
                continue

            #命令行元组
            cmd = ('tail', '-F', '-n{}'.format(self.line_num), self.path)

            #加载监控的关键字信息
            last_line = ""
            output = subprocess.Popen(cmd, stdout=subprocess.PIPE)
            while 1:
                try:
                    line = output.stdout.readline()
                    #判断新读入日志是否与上一条一样，重复则跳过
                    if line != last_line and line:
                        last_line = line
                    else:
                        continue

                    info = self.get_info(line)
                    note = {}
                    if info:
                        note.update(info)
                        db = pymongo.MongoClient(self.MONGO_HOST, self.MONGO_PORT)[self.db]
                        result = db[self.collection].insert_one(note)
                except:
                    if output:
                        output.terminate()
            if output:
                output.terminate()
        return

if __name__ == '__main__':
    near_cool = Cooler()
    path = '/Users/weather/git/collog/near_note_0.log.2015-09-02'

    #line = '2015-09-02 17:42:35,398 24572,Dummy-510 init_log.py:61 [NOTE] 6|GET|/topic_list2|api.haojin.in|200|0000|120.131.74.178|154ms|QMMWD/0.6.5 iPhone/8.4.1 AFNetwork/1.1|{"latitude": "39.995161", "bottom_id": "", "longitude": "116.474111", "pagesize": "10", "offset": "0"}|{"resperr": "", "respcd": "0000", "respmsg": "\u6210\u529f", "data": {"topics": [{"distance": 279, "topic_id": "55e523cab7ddb0a072fd2b6a", "type": 0, "head_image": "http://7xia80.com1.z0.glb.clouddn.com/topic_cate/img/4/ca74eb8a88c36cc1f488e52754a03709", "title": "\u3010\u94b1\u65b9\u00b7\u597d\u8fd1\u7f8e\u98df\u6708\u3011\u6ee110\u51cf10\uff0b\u6253\u6298\u518d\u51cf8\u5143\uff0b\u968f\u673a\u51cf\u6700\u9ad8\u514d\u5355"}, {"distance": 279, "topic_id": "55d69826b7ddb04108b76cc9", "type": 0, "head_image": "http://7xia80.com1.z0.glb.clouddn.com/topic_cate/img/3/71f3362b196f7aa3f3ffe39028967d03", "title": "\u3010\u63a8\u8350\u6709\u793c\u3011\u4f60\u5e0c\u671b\u54ea\u5bb6\u5e97\u52a0\u5165\u597d\u8fd1\u661f\u671f\u4e941\u5206\u94b1\u7684\u6d3b\u52a8\uff1f"}, {"distance": 33, "topic_id": "559cee23b7ddb0544bb750a0", "type": 0, "head_image": "http://7xia80.com1.z0.glb.clouddn.com/topic_cate/img/3/71f3362b196f7aa3f3ffe39028967d03", "title": "\u3010\u5b98\u65b9\u3011\u597d\u8fd1\u7248\u672c\u66f4\u65b0\u901a\u77e5"}, {"distance": 1671, "topic_id": "55e5644fc69575960b7f7f35", "type": 1, "head_image": "http://7xia80.com1.z0.glb.clouddn.com/topic_cate/img/4/ca74eb8a88c36cc1f488e52754a03709", "title": "\u3010\u9519\u5cf0\u5348\u9910\u3011\u4e50\u70e4\u97e9\u5f0f\u5927\u9171\u6c64\u5957\u9910\uff0c\u539f\u4ef730\uff0c\u73b0\u4ec516\u5143\uff01"}, {"distance": 26, "topic_id": "55e568cdc695759605cefe54", "type": 1, "head_image": "http://7xia80.com1.z0.glb.clouddn.com/topic_cate/img/4/ca74eb8a88c36cc1f488e52754a03709", "title": "\u3010\u7279\u5356\u65e9\u9910\u3011\u6cb9\u5634\u732b\u8c46\u6d46/\u6cb9\u6761/\u70e7\u997c/\u9e21\u86cb\u81ea\u9009\u5957\u9910\uff0c\u539f\u4ef711\uff0c\u73b0\u4ec55\u5143\uff01"}, {"distance": 925, "topic_id": "55e44fa3b7ddb02bc3525798", "type": 0, "head_image": "http://7xia80.com1.z0.glb.clouddn.com/topic_cate/img/4/ca74eb8a88c36cc1f488e52754a03709", "title": "\u4e50\u70e4\u6bcf\u59298\uff5e10\u4eba\u534a\u4ef7\u7279\u60e0\u70e7\u70e4\u7ec4\u5408\u5957\u9910\u9650\u91cf4\u4efd\uff0c\u5feb\u6765\u4f53\u9a8c\u5427"}, {"distance": 145, "topic_id": "55e417b8c6957562e61d4ae4", "type": 1, "head_image": "http://7xia80.com1.z0.glb.clouddn.com/topic_cate/img/4/ca74eb8a88c36cc1f488e52754a03709", "title": "\u3010\u7279\u5356\u5348\u9910\u3011ICC Angel\u756a\u8304\u57f9\u6839\u610f\u9762\u5957\u9910\uff0c\u539f\u4ef756\uff0c\u73b019.9\uff01"}, {"distance": 925, "topic_id": "55e42610c69575b400bd10d2", "type": 1, "head_image": "http://7xia80.com1.z0.glb.clouddn.com/topic_cate/img/4/ca74eb8a88c36cc1f488e52754a03709", "title": "\u3010\u4e0b\u5348\u8336/\u665a\u9910\u3011\u6f2b\u732b\u5564\u9152\u70b8\u9e21\u53cc\u4eba\u5957\u9910\uff0c\u539f\u4ef788\uff0c\u73b055\u5143\uff01"}, {"distance": 1671, "topic_id": "55e42071c69575b480fd77e8", "type": 1, "head_image": "http://7xia80.com1.z0.glb.clouddn.com/topic_cate/img/4/ca74eb8a88c36cc1f488e52754a03709", "title": "\u3010\u9519\u5cf0\u5348\u9910\u3011\u6ce1\u6ce1\u706b\u9505\u5355\u4eba\u706b\u9505\u5957\u9910\uff0c\u539f\u4ef729\uff0c\u73b015\u5143\uff01"}, {"distance": 925, "topic_id": "55dd8878c69575ac21e8c347", "type": 1, "head_image": "http://7xia80.com1.z0.glb.clouddn.com/topic_cate/img/4/ca74eb8a88c36cc1f488e52754a03709", "title": "\u4e0b\u5348\u8336\uff1a\u5706\u9999\u9187\u9999\u7eff\u5976\u8336\uff0c\u539f\u4ef716\u5143\uff0c\u60ca\u7206\u4ef76\u5143\uff01"}], "sale_topics": [{"distance": 925, "panic_good": {"status": 2, "start_time": "2015-09-06 11:00:19", "create_time": "2015-09-02 15:07:12", "server_time": "2015-09-02 17:42:35", "end_time": "2015-09-06 14:30:19"}, "attachment": {"item_id": 3905379, "attach_type": 1, "biz_id": "5570178ae635a7056c6fe049"}, "title": "\u3010\u9519\u5cf0\u5348\u9910\u3011\u5317\u6f02\u5c4c\u4e1d\u9762+\u53ef\u4e50\u5957\u9910\u539f\u4ef723\uff0c\u73b09.9\u5143", "topic_id": "55e6a074c695754e247269fd", "type": 1, "head_image": "http://7xia80.com1.z0.glb.clouddn.com/topic_cate/img/4/ca74eb8a88c36cc1f488e52754a03709"}]}}'
    #near_cool.get_info(line)
    #near_cool.setAttr("172.100.102.163", 27018, '/Users/weather/git/collog/t', 500, "bi", "note")
    #near_cool.importlog(path)
    #near_cool.run()