#coding:utf-8
'''
Created on 2015年12月14日

@author: xuqi
'''
import json
import urllib.request
import urllib.parse
import re
import os
import csv
from datetime import datetime
import threading
 
class MyQueue:
    def __init__(self):
        self.queue = []
        self.mutex = threading.Lock()
        self.condition = threading.Condition(self.mutex)
        self.decrease_num = 0
        self.unfinished_tasks = 0
        
    def put_item(self,item):
        with self.condition:
            self.queue.append(item)
            self.unfinished_tasks += 1
            self.condition.notify()

    
    def get_item(self,num=0):
        with self.condition:
            if(num==1):
                while(len(self.queue)==0):
                    self.condition.wait()
                self.decrease_num = 1
                item = self.queue.pop(0)
                self.condition.notify()
                return item,self.decrease_num  
            temp_list = []
            while(len(self.queue)==0):
                self.condition.wait()
                
            self.decrease_num = len(self.queue)
            for i in range(len(self.queue)):
                temp_list.append(self.queue.pop(0))
            self.condition.notify()
            return temp_list,self.decrease_num

    
    def task_done(self,count):
        with self.condition:
            unfinished = self.unfinished_tasks - count
            if(unfinished==0):
                self.condition.notify_all()
            self.unfinished_tasks = unfinished

    
    def join(self):
        with self.condition:
            while(self.unfinished_tasks):
                self.condition.wait()
              
        

class Productor(threading.Thread):
    def __init__(self,name,start_queue,temp_queue):
        threading.Thread.__init__(self,name=name)
        self.start_queue = start_queue
        self.temp_queue = temp_queue
        self.daemon = True
        
    def run(self):
        while True:
            poi,count = self.start_queue.get_item(1)
            baidu_gps = self.get_baidu_gps(poi)
            self.temp_queue.put_item(baidu_gps)
            self.start_queue.task_done(count)
            

   
    def get_baidu_gps(self,poi):
        ak = 'rj3VEq9SOy7zGndNifOoPIXW'
        host_url = 'http://api.map.baidu.com/geocoder/v2/?address=ADDRESS_NAME&output=json&ak=%s&callback=showLocation'%ak
        address_name = poi['chg_dz']
        address = urllib.parse.quote(address_name)
        tmp_url = host_url.replace('ADDRESS_NAME',address)
        req = urllib.request.Request(tmp_url)
        try:
            data=urllib.request.urlopen(req)
        except:
            data=urllib.request.urlopen(req)
        rs= data.read()
        print(rs)
        
        text=rs.decode('gbk')
    
        re_pattern = re.compile('showLocation&&showLocation\((.*)\)')
        match = re_pattern.search(text)
    
        poi['gis_x'] = ''
        poi['gis_y'] = ''
        if match:
            try:
                tmp = json.loads(match.group(1))
                poi['gis_x'] = tmp['result']['location']['lng']
                poi['gis_y'] = tmp['result']['location']['lat']
                return poi
            except:
                return poi
            
        else:
            return poi
        
class Consumer(threading.Thread):
    def  __init__(self,name,temp_queue,out_list):
        threading.Thread.__init__(self,name=name)
        self.temp_queue = temp_queue
        self.out_list = out_list
        self.daemon = True
        
    def run(self):
        while True:
            temp_pois,count = self.temp_queue.get_item()
            wgs = self.get_wgs84_gps(temp_pois)
            print(len(wgs))
            self.out_list.extend(wgs)
            self.temp_queue.task_done(count)
         
            
    def calculate_wgs84_gps(self,tmp_gps, baidu_gps):
        results = []
        for i in range(len(tmp_gps)):
            try:
                tmp_gps[i]['gis_x'] = float(tmp_gps[i]['gis_x']) * 2 - baidu_gps[i]['x']
                tmp_gps[i]['gis_y'] = float(tmp_gps[i]['gis_y']) * 2 - baidu_gps[i]['y']
                results.append(tmp_gps[i])
            except:
                results.append(tmp_gps[i])
        return results

            
    def wgs84_to_baidu(self,coords):
        ak = 'rj3VEq9SOy7zGndNifOoPIXW'
        url_template = 'http://api.map.baidu.com/geoconv/v1/?coords=coords_args&from=1&to=5&ak=%s' % ak
        if coords:
            coords_args = ";".join(coords)
            url = url_template.replace('coords_args', coords_args)
            req = urllib.request.Request(url)
            try:
                data=urllib.request.urlopen(req)
            except:
                data=urllib.request.urlopen(req)
            rs= data.read()
        
            text=rs.decode('gbk')
            request_result = json.loads(text)
            return request_result['result']
        else:
            return []
    
    def get_wgs84_gps(self,pois):
        gis_result = []
        coords = []
        empty_gis = []
        full_gis = []
        if(len(pois)==0):
            return gis_result
        for i in range(len(pois)):
            if(pois[i]['gis_x']!=''):
                coords.append('%s,%s' % (pois[i]['gis_x'], pois[i]['gis_y']))
                full_gis.append(pois[i])
            else:
                empty_gis.append(pois[i])
            
        result_poi = self.wgs84_to_baidu(coords)
        wgs = self.calculate_wgs84_gps(full_gis, result_poi)
        gis_result.extend(wgs)
        gis_result.extend(empty_gis)
        return gis_result
        
def read_json(path):
    with open(path,mode='r',encoding='utf-8') as fp:
        data = fp.read()
        data = data.replace('\n','')
        data = json.loads(data)
        return data    
def read_csv(path):
    with open(path,'r') as f:
        reader = csv.reader(f)
        reader = list(reader)
        columns = [t.lower() for t in reader[0]]
        result = [dict(zip(columns,item)) for item in reader[1:] if item != []]
        return result   

def save_csv(path,datalist):              
    with open(path,'w',newline='') as f:
        writer = csv.writer(f)
        writer.writerow(list(datalist[0].keys()))
        for item in datalist:
            writer.writerow(list(item.values()))
            
def poi_gis(filename,productor_num,consumer_num):
    start_time = datetime.now()
    
    start_queue = MyQueue()
    temp_queue = MyQueue()
    out_list = []
    data = read_csv(filename)
    
        
    for addr in data:
        start_queue.put_item(addr)
        
    productor_pool = []
    for i in range(productor_num):
        productor_pool.append(Productor('gps_productor_%s'%i,start_queue,temp_queue))
    
    consumer_pool = []
    for i in range(consumer_num):
        consumer_pool.append(Consumer('gps_consumer_%s'%i,temp_queue,out_list))
    
    for i in range(productor_num):
        productor_pool[i].start()
    
    for i in range(consumer_num):
        consumer_pool[i].start()
    
    start_queue.join()
    temp_queue.join()
    
        
    #queue2csv(r'C:\Users\xuqi\Desktop\bd_%s'%(os.path.splitext(filename)[0]+'.csv'),temp_que)
    print(len(out_list))
    save_csv(r'result.csv',out_list)
    #print('Done')             
    print(datetime.now()-start_time) 
                                  
if __name__ == "__main__":
    poi_gis(r'500or501_1.csv',100,5)
    
    






    