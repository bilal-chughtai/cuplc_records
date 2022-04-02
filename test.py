import urllib.request
import json
import sqlalchemy as db
import csv
from datetime import datetime
from sqlalchemy import Table, Column, Float, String, Date, ForeignKey, Boolean, delete, select, func

def toDate(date_string):
    return datetime.strptime(date_string,'%Y-%m-%d')


engine = db.create_engine('sqlite:///data/database.db', future=True)
connection = engine.connect()
metadata_obj = db.MetaData()

lifters_table = Table(
        "lifters",
        metadata_obj,
        Column('id', String, primary_key=True),
        Column('fullName', String),
        Column('sex', String),
        Column('matricDate', Date),
        Column('gradDate', Date)
)

results_table = Table(
        "results",
        metadata_obj,
        Column('id', String, primary_key=True),
        Column('lifter_id', ForeignKey('lifters.id')),
        Column('student', Boolean),
        Column('alumni', Boolean), # need both in case people competeed before uni
        Column('meetName', String),
        Column('meetDate', Date),
        Column('bodyweight', Float),
        Column('squat', Float, nullable=True),
        Column('bench', Float, nullable=True),
        Column('deadlift', Float, nullable=True),
        Column('total', Float, nullable=True)
        
)

record_table = Table(
        "records",
        metadata_obj,
        Column('weightclass', String),
        Column('sex', String),
        Column('student', Boolean),
        Column('alumni', Boolean),
        Column('type', String),
        Column('result_id', String),
)


metadata_obj.create_all(engine)

lifter_file = open("lifters.json")
lifters = json.load(lifter_file)

for lifter in lifters:
    stmt = db.insert(lifters_table).values(id=lifter['id'], fullName=lifter['fullName'], sex=lifter['sex'], matricDate=toDate(lifter['matricDate']), gradDate=toDate(lifter['gradDate'])).prefix_with("OR REPLACE")
    result = connection.execute(stmt)  
    csv_file = f"data/{lifter['id']}.csv"
    urllib.request.urlretrieve(f"https://www.openpowerlifting.org/u/{lifter['id']}/csv", csv_file)
    with open(csv_file, mode='r') as data:
        csv_reader = csv.DictReader(data)
        for row in csv_reader:
            if row['Equipment'] != 'Raw':
                continue
            student=False
            alumni=False
            if toDate(lifter['matricDate']) <= toDate(row['Date']) <= toDate(lifter['gradDate']):
                student=True
            elif toDate(lifter['gradDate']) < toDate(row['Date']):
                alumni=True
            for key in row:
                if row[key] == '':
                    row[key]=0
            stmt = db.insert(results_table).values(id=lifter['id']+row['Date'], lifter_id=lifter['id'], student=student, alumni=alumni, meetName=row['MeetName'], meetDate=toDate(row['Date']), bodyweight=row['BodyweightKg'], squat=row['Best3SquatKg'], bench=row['Best3BenchKg'], deadlift=row['Best3DeadliftKg'], total=row['TotalKg']).prefix_with("OR REPLACE")
            result = connection.execute(stmt)
    connection.commit()


stmt=delete(record_table)
result = connection.execute(stmt)


female_classes = [47, 52, 57, 63, 69, 76, 84]
male_classes = [59, 66, 74, 83, 93, 105, 120]
lifts = ['squat', 'bench', 'deadlift', 'total']

def class_boundaries_to_classes(boundaries):
    number_of_classes = len(boundaries)
    classes = [(str(boundaries[0])+'kg', 0, boundaries[0])] # name, lower, upper
    for i in range(1,number_of_classes):
        classes.append((str(boundaries[i])+'kg', boundaries[i-1], boundaries[i]))
    classes.append((str(boundaries[number_of_classes-1])+'kg+', boundaries[number_of_classes-1],1000))
    return classes

female_classes = class_boundaries_to_classes(female_classes)
male_classes = class_boundaries_to_classes (male_classes)

sexes = ['M', 'F']
classes = {'M': male_classes, 'F':female_classes}
statuses = ['student', 'alumni']

for sex in sexes:
    for status in statuses:
        for weight_class in classes[sex]:
            for lift in lifts:
                stmt = select(results_table.c.id, func.max(getattr(results_table.c,lift))).where(lifters_table.c.sex == sex).where(getattr(results_table.c,status) == True).where(weight_class[1] < results_table.c.bodyweight).where(results_table.c.bodyweight<= weight_class[2]).join_from(results_table, lifters_table)
                valid_results = connection.execute(stmt)
                i=0
                for row in valid_results:
                    i+=1
                    if status=='student':
                        student=True
                        alumni=False
                    elif status=='alumni':
                        alumni=True
                        student=False
                    stmt = db.insert(record_table).values(weightclass=weight_class[0], sex=sex, student=student, alumni=alumni, type=lift, result_id=row[0])
                    connection.execute(stmt)
            connection.commit()            
                

