'''
{'xzqy': '320583', 'jzdz': '千灯镇长江路东，长丰路南', 'gis_y': '31.4043035', 'gis_x': '150.9755925'}
常熟行政代码为320581 昆山行政代码为320583
if 'xzqy' == '320583' 并且'jzdz'中无昆山和昆山市
则在'jzdz'中加入'昆山市'
结果如：{'chg_dz': '昆山市千灯镇长江路东，长丰路南', 'xzqy': '320583', 'jzdz': '千灯镇长江路东，长丰路南', 'gis_y': '31.4043035', 'gis_x': '150.9755925'}
'''
import csv

def read_csv(path):
    with open(path,'r') as f:
        reader = csv.reader(f)
        reader = list(reader)
        columns = [t.lower() for t in reader[0]]
        result = [dict(zip(columns,item)) for item in reader[1:] if item != []]
        return result

def add_address(result):
    result_0 = []
    for i in range(len(result)):
        if ('苏州' or '沧浪' not in result[i]['dwdz']):
            result[i]['chg_dz'] = '苏州沧浪区' + result[i]['dwdz']
        else:
            result[i]['chg_dz'] = result[i]['dwdz']
        result_0.append(result[i])

    return result_0

def select_it(result):
    result_1 = []
    for i in range(len(result)):
        #result[i]['gis_x'] = ''.join(result[i]['gis_x'].split())
        #result[i]['gis_y'] = ''.join(result[i]['gis_y'].split())
        if result[i]['gis_x'] == '' or result[i]['gis_y'] == '' or result[i]['gis_x'] == '0' or result[i]['gis_y'] == '0':
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
    read_result = read_csv(r'502.csv')
    print(read_result[0])

    temp = add_address(read_result)
    temp1 = select_it(temp)

    save_csv(r'502_1.csv',temp1)
    print('done!!!')
