#!/usr/bin/python

import pymysql.cursors
import _thread
import time
import queue
import csv


# Connect to the database


connection = pymysql.connect(host='localhost',
                             user='root',
                             password='maruf123',
                             db='sourcerer',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)


def get_file_by_rhs(entities, file_id):
    result = []
    
    try:    
        ent = []
        for row in entities:
            ent.append(row['entity_id'])
        format_strings = ','.join(['%s'] * len(ent))
        
        with connection.cursor() as cursor:
                cursor.execute("SELECT DISTINCT `file_id` FROM `relations` WHERE relation_type in ('CALLS','INSTANTITIATES') AND relation_class = 'INTERNAL'  AND rhs_eid in (%s)"% (format_strings% tuple(ent))) 
                result = cursor.fetchall()
                
    except Exception as e: print(e)
      
    return result


def get_lhs_by_rhs(rhs):
    result = []
    try:    
        with connection.cursor() as cursor:
                cursor.execute("SELECT DISTINCT `lhs_eid` FROM `relations` WHERE relation_type in ('CALLS','INSTANTITIATES') and `relation_class` = 'INTERNAL' and `rhs_eid` = %s"%rhs) 
                result = cursor.fetchall()
    except Exception as e: print(e)
        
    return result



def get_entity_by_file(file_id):
    result = []

    try:
        with connection.cursor() as cursor:

                sql = "SELECT `entity_id` FROM `entities` WHERE `file_id` = %s and (`entity_type` = 'METHOD'  or `entity_type`='CONSTRUCTOR')" 
                cursor.execute(sql,(file_id))
                result = cursor.fetchall()
    except Exception as e: print(e)
    
    return result



def file_based(file_id):   
    
    q = queue.Queue()
    seen = []
    q.put(file_id)
    seen.append(file_id)
    count = 1
        
    while not q.empty():
        id = q.get()
        #print(seen)
        entities = get_entity_by_file(id)
        files = get_file_by_rhs(entities, id)
        for row in files:    
            if (row['file_id'] not in seen) and (row['file_id'] != None):
                q.put(row['file_id'])
                seen.append(row['file_id'])
                count = count + 1    
    #print(count,"   seen : ",len(seen))
    return count


def get_file_by_entity(ent):
    result = []
    try:    
        format_strings = ','.join(['%s'] * len(ent))
        
        with connection.cursor() as cursor:
                cursor.execute("SELECT DISTINCT `file_id` FROM `entities` WHERE `entity_id` in (%s)"% (format_strings% tuple(ent))) 
                result = cursor.fetchall()
    except Exception as e: print(e)
        
    return result


def entity_based(file_id, entity_id):
    q = queue.Queue()
    q.put(entity_id)
    count = 0;
    seen = []
    seen.append(entity_id)
    while not q.empty():
        id = q.get()
        entities = get_lhs_by_rhs(id)
        #print(entities)
        for row in entities:
            if row['lhs_eid'] not in seen:
                q.put(row['lhs_eid'])
                seen.append(row['lhs_eid'])
    
    files = get_file_by_entity(seen)            
    
    count = len(files)
    return count


if __name__ == '__main__':
    result = []
    try:
        with connection.cursor() as cursor:
                
                sql = "SELECT `file_id`, `project_id` FROM `files` WHERE (`project_id` >= 24 and `project_id` <= 71) and `name` not like '%.jar%'" 
                cursor.execute(sql,)
                result = cursor.fetchall()   
                file = 'data_log'
                file = file + '.csv'              
                with open(file,'w') as f1:
                    writer=csv.writer(f1, delimiter=',',lineterminator='\n',)
                    header = ['project_id','file_id','entity_id','file_based','entity_based']
                    writer.writerow(header)
                    if result:
                        for row in result:
                            sql = "SELECT `entity_id` FROM `entities` WHERE `file_id` = %s and (`entity_type` = 'METHOD' or `entity_type` = 'CONSTRUCTOR' )" 
                            cursor.execute(sql,(row['file_id'],))
                            result1 = cursor.fetchall()
                            count = file_based(row['file_id'])
                            
                            for row1 in result1:
                                csvdata = []
                                
                                count1 = entity_based(row['file_id'], row1['entity_id'])
                                if count1 > 0:
                                    #print('\n')
                                    csvdata.append(row['project_id']) 
                                    csvdata.append(row['file_id']) 
                                    csvdata.append(row1['entity_id'])
                                    print("for file : ",row['file_id'], " for entity ", row1['entity_id']," we need to compile : ", (count1 ), " files ")
                                    
                                    print("for file : ",row['file_id'], " we need to compile : ", count, " files ")

                                    csvdata.append(count)
                                    csvdata.append(count1)
                                    writer.writerow(csvdata)
    finally:
        if connection.open:
            connection.close()
        
    