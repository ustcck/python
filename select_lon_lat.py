'''
判断经度是否属于【120，122）
判断纬度是否属于【30，33）
'''

import csv

def read_csv(path):
    with open(path,'r',) as f:
        reader = csv.reader(f)
        reader = list(reader)
        columns = [t.lower() for t in reader[0]]
        result = [dict(zip(columns,item)) for item in reader[1:] if item != []]
        return result

def select_it(result):
    result_1 = []
    for i in range(len(result)):
        #result[i]['gis_x'] = ''.join(result[i]['gis_x'].split())
        #result[i]['gis_y'] = ''.join(result[i]['gis_y'].split())
        if result[i]['gis_x'] == '' or result[i]['gis_y'] == '':
            result[i]['gis_x'] = ''
            result[i]['gis_y'] = ''
        #if float(result[i]['gis_y']) <30.0 or float(result[i]['gis_y']) >= 33.0:
        elif float(result[i]['gis_x']) < 120.0 or float(result[i]['gis_x']) >= 122.0 or float(result[i]['gis_y']) < 30.0 or float(result[i]['gis_y']) >= 33.0:
            result[i]['gis_x'] = ''
            result[i]['gis_y'] = ''

        result_1.append(result[i])
    return result_1

def save_csv(path,datalist):              
    with open(path,'w',newline='') as f:
        writer = csv.writer(f)
        writer.writerow(list(datalist[0].keys()))
        for item in datalist:
            writer.writerow(list(item.values()))


if __name__ == "__main__":
    path='CHANGSHU_DWXX.csv'
    read_result = read_csv(path)
    print(read_result[0])

    temp = select_it(read_result)
    save_csv(r'changshu_result_110.csv',temp)
