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
import math
from datetime import datetime

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
            
            
def get_distence(lon1,lat1,lon2,lat2):
    radius = 6371
    lon1,lat1,lon2,lat2 = list(map(math.radians,[lon1,lat1,lon2,lat2]))
    dlon = lon2-lon1
    dlat = lat2-lat1
    a = math.sin(dlat/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin(dlon/2)**2
    c = 2*math.asin(math.sqrt(a))
    d = c*radius
    return float('%.6f'%float(d))

def get_min_distence(addrlist,matchlist):
    
    gis_results = []
    for addr in addrlist:
        start = datetime.now()
        gis_dict = {}
        min_distence = float('inf')
        try:
            for match in matchlist:
                distence = get_distence(float(addr['gis_x']),float(addr['gis_y']),
                                    float(match['xfjd_gis_x']),float(match['xfjd_gis_y']))
                if(distence<min_distence):
                    min_distence = distence
                    gis_dict['hzbg_dz'] = addr['dwdz']
                    gis_dict['xfjd_dz'] = match['dwdz']
                    gis_dict['gis_x'] = addr['gis_x']
                    gis_dict['gis_y'] = addr['gis_y']
                    gis_dict['dwmc'] = match['dwmc']
                    gis_dict['dwid'] = match['dwid']
                    gis_dict['hz_dwmc'] = addr['dwmc']
                    gis_dict['min_dist'] = min_distence
            gis_results.append(gis_dict)
        except ValueError:
            pass       
        print(datetime.now()-start)
    return gis_results

def get_baidu_gps(poi):
    ak = 'rj3VEq9SOy7zGndNifOoPIXW'
    host_url = 'http://api.map.baidu.com/geocoder/v2/?address=ADDRESS_NAME&output=json&ak=%s&callback=showLocation'%ak
    print (poi)
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


def calculate_wgs84_gps(tmp_gps, baidu_gps):
    results = []
    for i in range(len(tmp_gps)):
        try:
            tmp_gps[i]['gis_x'] = float(tmp_gps[i]['gis_x']) * 2 - baidu_gps[i]['x']
            tmp_gps[i]['gis_y'] = float(tmp_gps[i]['gis_y']) * 2 - baidu_gps[i]['y']
            results.append(tmp_gps[i])
        except:
            results.append(tmp_gps[i])
    return results

            
def wgs84_to_baidu(coords):
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
    
def get_wgs84_gps(pois):
    gis_result = []
    coords = []
    empty_gis = []
    full_gis = []
    for i in range(len(pois)):
        if(pois[i]['gis_x']!=''):
            coords.append('%s,%s' % (pois[i]['gis_x'], pois[i]['gis_y']))
            full_gis.append(pois[i])
        else:
            empty_gis.append(pois[i])
        if(i==0):
            continue
        if(i%10==0):
            result_poi = wgs84_to_baidu(coords)
            wgs = calculate_wgs84_gps(full_gis, result_poi)
            print(wgs[0])
            gis_result.extend(wgs)
            coords = []
            full_gis = []
            
    result_poi = wgs84_to_baidu(coords)
    wgs = calculate_wgs84_gps(full_gis, result_poi)
    gis_result.extend(wgs)
    gis_result.extend(empty_gis)
    return gis_result

def poi_gis(filename):
    data = read_csv(filename)
    temp = []
    for addr in data:
        item = get_baidu_gps(addr)
        temp.append(item)
    save_csv(r'500or501_1_1.csv',temp)
    chg_data = get_wgs84_gps(temp)
    save_csv(r'500or501_1_2.csv',chg_data)
    print('Done')
      
def try_poi_gis(filename):
    temp = read_csv(r'C:\Users\xuqi\Desktop\%s'%(os.path.splitext(filename)[0]+'.csv'))
    chg_data = get_wgs84_gps(temp)
    save_csv(r'C:\Users\xuqi\Desktop\fin_%s'%(os.path.splitext(filename)[0][3:]+'.csv'),chg_data)
    print('Done') 

def test_poi(address):
    data = {'chg_dz':address}           
    poi = get_baidu_gps(data)
    print(poi)
    result_poi = wgs84_to_baidu('%s,%s'%(poi['gis_x'],poi['gis_y']))
    print(calculate_wgs84_gps(poi, result_poi))


                                   
if __name__ == "__main__":
    poi_gis('500or501_1.csv')
    #try_poi_gis('bd_ks_jzdz.csv')





    