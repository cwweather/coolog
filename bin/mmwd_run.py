#!/usr/bin/env python
# encoding: utf-8
import datetime
import json
import re
import urlparse

from cooler import Cooler

pattern1 = "(\d+-\d+-\d+ \d+:\d+:\d+),\S+ \S+ \S+ \[NOTE\] openid=(\S+) near_uid=(\d+) ip=(\S+) method=(\S+) path=(\S+) query= body=(.+) status=(\S+) time=(\S+) len=(\S+) ua=(.+) ret=(\{.+\}) ref="
pattern2 = "(\d+-\d+-\d+ \d+:\d+:\d+),\S+ \S+ \S+ \[NOTE\] openid=(\S+) near_uid=(\d+) ip=(\S+) method=(\S+) path=(\S+) query=(\S+)? status=(\S+) time=(\S+) len=(\S+) ua=(.+) ret=(\{.+\}) ref="

class MmwdCooler(Cooler):

    def settings(self):
        self.MONGO_HOST = "192.20.20.11"
        self.MONGO_PORT = 27017
        self.path = '/home/qfpay/weidian_mmwd/log/weidian_mmwd_0.log'
        self.line_num = 0
        self.db = "bi"
        self.collection = "note"

    def get_info(self, line):
        note = {}
        try:
            infos = re.match(pattern1, line)
            if not infos:
                infos = re.match(pattern2, line)
            if infos:
                args = infos.group(7).strip()
                try:
                    args = json.loads(args)
                except:
                    args = dict([(k, v[0]) for k, v in urlparse.parse_qs(args).items()])
                try:
                    ret = json.loads(infos.group(12).strip())
                    respcd = ret["respcd"]
                except:
                    ret = infos.group(12).strip()
                    if "\"respcd\":\"0000\"" in ret:
                        respcd = "0000"
                    else:
                        respcd = ""
                note = {
                    "user_id": infos.group(3),
                    "method": infos.group(5),
                    "path": infos.group(6),
                    "status": infos.group(8),
                    "respcd": respcd,
                    "ret": ret,
                    "remote_ip": infos.group(4),
                    "duration": infos.group(9),
                    "UA": infos.group(11),
                    "args": args,
                    "datetime": datetime.datetime.strptime(infos.group(1), "%Y-%m-%d %H:%M:%S")
                }
            else:
                infos = re.match(pattern2, line)
                try:
                    args = json.loads(infos.group(7).strip())
                except:
                    args = infos.group(7).strip()
                try:
                    ret = json.loads(infos.group(12).strip())
                    respcd = ret["respcd"]
                except:
                    ret = infos.group(12).strip()
                    respcd = ""
                note = {
                    "user_id": infos.group(3),
                    "method": infos.group(5),
                    "path": infos.group(6),
                    "status": infos.group(8),
                    "respcd": respcd,
                    "ret": ret,
                    "remote_ip": infos.group(4),
                    "duration": infos.group(9),
                    "UA": infos.group(11),
                    "args": args,
                    "datetime": datetime.datetime.strptime(infos.group(1), "%Y-%m-%d %H:%M:%S")
                }
            if note["path"] not in ["/api/order/buy", "/api/order/qtpay_ispayed", "/api/order/cdc_pay",
                                    "/api/order/activity_buy", "/api/order/paybill", "/api/order/special_sale_buy",
                                    "/api/order/take_out_buy"]:
                return {}
        except:
            pass
        return note

if __name__ == '__main__':
    cool = MmwdCooler()
    #print cool.get_info('2015-08-19 12:18:16,835 10105,Dummy-102 main.py:162 [NOTE] openid=wd_fa91b072-45a6-11e5-9532-74867af2b490 near_uid=26 ip=120.131.74.178 method=POST path=/api/order/buy query= body={"buy_feature":"haojin","items":[{"skuid":"4233061","itemid":"3921610","amount":"1"}],"cus_address":{"consignee":" ","tel":"18810813966"},"topic_id":"55d151b6c69575827fa7a30d"} status=200 time=204.1 len=322 ua=QMMWD/0.6.3 iPhone/8.4 AFNetwork/1.1 ret={"resperr":"","respcd":"0000","respmsg":"","data":{"goods_name":"\u5317\u4eac","total_amt":1,"mchnt_name":"\u718a \u5927 \u8d85 \u5e02\u3010V\u3011","consignee":"","address":"","count":1,"mobile":"18810813966","order_token":"06e20bdf20634f26aa9ea0e671ade92d","order_id":461551,"pay_amt":1,"goods_info":"","good_amount":1}} ref=')
    #print cool.get_info('2015-08-19 14:46:05,267 21677,Dummy-569 main.py:162 [NOTE] openid=wd_ea3fa34a-463d-11e5-926c-74867af2aa20 near_uid=104479 ip=124.205.181.88 method=POST path=/api/order/buy query= body={"buy_feature":"haojin","items":[{"skuid":"4235583","itemid":"3924191","amount":"1"}],"cus_address":{"consignee":" ","tel":"18510089192"},"topic_id":"55d311efc695751774a704c1"} status=200 time=140.3 len=321 ua=QMMWD/0.4.1 iPhone/8.4.1 AFNetwork/1.1 ret={"resperr":"","respcd":"0000","respmsg":"","data":{"goods_name":"V Coffee\u5496\u5561\u4e09\u9009\u4e00","total_amt":800,"mchnt_name":"vcoffee","consignee":"","address":"","count":1,"mobile":"18510089192","order_token":"f0fd2b9e48ca45668f8911a8e6adca1f","order_id":461721,"pay_amt":800,"goods_info":"","good_amount":800}} ref=')
    #print cool.get_info('2015-08-19 14:53:54,263 40680,Dummy-989 main.py:162 [NOTE] openid=wd_8be9cdba-4639-11e5-b07c-74867af2b490 near_uid=1 ip=120.131.74.178 method=GET path=/api/order/qtpay_ispayed query=busi_type=1&qt_order_id=6039660298509692766 status=200 time=21.7 len=85 ua=QMMWD/0.6.2 iPhone/8.4 AFNetwork/1.1 ret={"resperr":"","respcd":"0000","respmsg":"","data":{"order_id":461731,"order_type":6}} ref=')
    #print cool.get_info('2015-08-19 16:04:50,863 12680,Dummy-266 main.py:162 [NOTE] openid=wd_992bee82-4647-11e5-9cf0-74867af2b490 near_uid=10088 ip=120.131.74.178 method=POST path=/api/order/activity_buy query= body={"amount":"1","activity_id":"2","addr_id":"10023","remark":"UK百年孤寂NB"} status=200 time=146.6 len=551 ua=QMMWD/0.6.2 iPhone/8.3 AFNetwork/1.1 ret={"resperr":"","respcd":"0000","respmsg":"","data":{"goods_name":"\u679c\u7136\u7231-\u6c34\u679c\u5207","total_amt":1,"mchnt_name":"\u67da\u5b50\u7f8e\u8863\u670d\u88c5\u5e97","consignee":"\u5706\u5706 \u539f\u539f","address":"\u671b\u4eacSOHO T3A\u5ea7 17\u5c42 \u94b1\u65b9\u516b\u4f70\u4f34\u5251\u53ca\u5c65\u53cakg\u57fa\u91cc\u8fde\u79d1\u54c8\u54c8\u54c8\u767e\u53e3\u83ab\u8fa9\u4f60","count":1,"mobile":"18310815866","order_token":"4a40dc07319f48ee96cd30d3a46bb760","order_id":461814,"pay_amt":1,"pay_flag":0,"goods_info":"","good_amount":1}} ref=')
    #print cool.get_info('2015-08-24 09:41:21,539 38708,Dummy-498 main.py:162 [NOTE] openid=wd_38dd369a-4a01-11e5-aeb0-74867af2b490 near_uid=104630 ip=117.136.38.24 method=POST path=/api/order/cdc_pay query= body=discount_pay=1.20&shop_id=5570178be635a7056c6fe04a&original_pay=12.0&real_pay=10.80 status=200 time=133.5 len=487 ua=NEAR/1.3.7 Android/4.4.4 Device/SM-A5000 ret={"resperr":"","respcd":"0000","respmsg":"","data":{"goods_name":"\u591a\u90a3\u4e4b\u5496\u5561\u86cb\u7cd5\u70d8\u7119","order_id":463793,"mchnt_name":"\u591a\u90a3\u4e4b\u5496\u5561\u86cb\u7cd5\u70d8\u7119","consignee":"","memo":"{\"discount\": \"9\", \"discount_pay\": 1.2, \"card_id\": 100188}","user_token":"7e05f5f49ee0494faedfc22977cb21da","address":"","count":1,"order_token":"13b0fa0fd04149c0a031badef55ddbe4","total_amt":1080,"pay_amt":1080,"goods_info":"","good_amount":1080}} ref=')
    cool.run()
