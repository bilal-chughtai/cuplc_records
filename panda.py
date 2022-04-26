from re import M
import re
import pandas as pd
import json
from datetime import datetime



def toDate(date_string):
    return datetime.strptime(date_string,'%Y-%m-%d')



# create dataframe holding information about lifters

lifters_columns = ['id', 'fullName', 'sex', 'matricDate', 'gradDate']
lifters_data = []
lifter_file = open("lifters.json")
lifters_dump = json.load(lifter_file)
for lifter in lifters_dump:
    row = [lifter['id'], lifter['fullName'], lifter['sex'], toDate(lifter['matricDate']), toDate(lifter['gradDate'])]
    lifters_data.append(row)
lifters = pd.DataFrame(lifters_data, columns=lifters_columns)


# get results from openpowerlifting and create dataframe

results_columns = ['id', 'lifter_id', 'student', 'alumni', 'meetName', 'meetDate', 'bodyweight', 'squat', 'bench', 'deadlift', 'total']
drop_columns = ['Name', 'Sex',	'Event', 'Equipment',	'Age',	'AgeClass',	'BirthYearClass',	'Division',	'WeightClassKg'	,'Squat1Kg',	'Squat2Kg',	'Squat3Kg',	'Squat4Kg',	'Bench1Kg',	'Bench2Kg',	'Bench3Kg',	'Bench4Kg',		'Deadlift1Kg',	'Deadlift2Kg',	'Deadlift3Kg',	'Deadlift4Kg',		'Place',	'Dots',	'Wilks',	'Glossbrenner',	'Goodlift',	'Tested',	'Country',	'State',	'Federation',	'ParentFederation',	'MeetCountry',	'MeetState',	'MeetTown']
rename_map = {
    'BodyweightKg': 'bodyweight',
    'Best3SquatKg': 'squat',
    'Best3BenchKg': 'bench',
    'Best3DeadliftKg': 'deadlift',
    'TotalKg': 'total',
    'MeetName': 'meetName',
    'Date': 'meetDate'
}

frames = []

def get_status(meetDate, matricDate, gradDate):
    if matricDate <= toDate(meetDate) <= gradDate:
        return 'student'
    elif gradDate < toDate(meetDate):
        return 'alumni'
    return 'none'

for index, lifter in lifters.iterrows():
    df = pd.read_csv(f"https://www.openpowerlifting.org/u/{lifter['id']}/csv")
    df.drop(columns=drop_columns, inplace=True)
    df.rename(mapper=rename_map, axis=1, inplace=True)
    df['lifter_id'] = lifter['id']
    df['id']=df['lifter_id']+df['meetDate']
    df['status'] = df.apply(lambda x: get_status(x['meetDate'], lifter['matricDate'], lifter['gradDate']), axis=1)
    frames.append(df)

results = pd.concat(frames)



# join results and lifters

data = results.merge(lifters, how="left", left_on="lifter_id", right_on="id")
print(data)


# generate records

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
record_dump = []
record_log = []

record_columns = ['sex', 'status', 'weightclass', 'lift', 'fullName', 'liftKg', 'date']
old_records  = pd.read_csv('records.csv', names=record_columns)

for sex in sexes:
    for status in statuses:
        for weight_class in classes[sex]:

                valid_results = data[
                    (data['sex']==sex) &
                    (data['status']==status) &
                    (weight_class[1] < data['bodyweight']) &
                    (data['bodyweight'] <= weight_class[2])
                ]


                for lift in lifts:
                    
                    # get old record
                    old = old_records[
                        (old_records['sex']==sex) &
                        (old_records['status']==status) &
                        (old_records['weightclass']==weight_class[0]) &
                        (old_records['lift']==lift)]
                    
                    oldKg = 0
                    for index,row in old.iterrows():
                        oldKg = row['liftKg']

                    # compute new record
                    maxes = valid_results[valid_results[lift]==valid_results[lift].max()]
                    earliest = maxes[maxes['meetDate']==maxes['meetDate'].min()]
                    for index,row in earliest.iterrows():
                        record = [sex, status, weight_class[0], lift, row['fullName'], row[lift], row['meetDate']]
                        record_dump.append(record)
                        if row[lift] > float(oldKg):
                            record_log.append(f"{row['meetDate']}: New {status} {sex}{weight_class[0]} record of {row[lift]} (+{row[lift]-oldKg}) by {row['fullName']} at {row['meetName']}")

# dump record and logs to file
records=pd.DataFrame(data=record_dump, columns=record_columns)
print(records)
records.to_csv('records.csv')
with open('log.txt', 'w') as f:
    for item in record_log:
        f.write("%s\n" % item)
