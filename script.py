import pandas as pd
import json
from datetime import datetime
import gspread
import df2gspread.df2gspread as d2g
import df2gspread.gspread2df as g2d
from oauth2client.service_account import ServiceAccountCredentials


def toDate(date_string):
    return datetime.strptime(date_string,'%Y-%m-%d')

def get_status(meetDate, matricDate, gradDate):
    if matricDate <= meetDate <= gradDate:
        return 'student'
    elif gradDate < meetDate:
        return 'alumni'
    return 'none'

def validate_lifter(lifter):
    if lifter['id'] == '':
        return 'id is empty'
    if lifter['fullName'] == '':
        return 'fullName is empty'
    if lifter['sex'] not in ['M', 'F']:
        return 'invalid sex'
    # check form of dates
    try:
        toDate(lifter['matricDate'])
    except:
        return 'invalid matricDate'
    try:
        toDate(lifter['gradDate'])
    except:
        return 'invalid gradDate'
    # check that matricDate is before gradDate
    if toDate(lifter['matricDate']) > toDate(lifter['gradDate']):
        return 'matricDate is after gradDate'
    # check that if skip_opl is false or blank, then the openpowerlifting link exists
    if not lifter['skip_opl'] or lifter['skip_opl'] == '':
        try:
            df = pd.read_csv(f"https://www.openpowerlifting.org/api/liftercsv/{lifter['id']}")
        except:
            return 'openpowerlifting id does not exist'
    return True

def validate_manual_result(row, lifters):
    if row['lifter_id'] == '':
        return 'lifter_id is empty'
    if row['lifter_id'] not in lifters['id'].values:
        return 'lifter_id not in lifters'
    if row['meetName'] == '':
        return 'meetName is empty'
    # check form of dates
    try:
        toDate(row['meetDate'])
    except:
        return 'invalid meetDate'
    return True

# load config json
with open('config.json') as json_file:
    cfg = json.load(json_file)

# google drive credentials
scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(cfg['service_account'], scope)
gc = gspread.authorize(credentials)

# create dataframe holding information about lifters
lifters = g2d.download(cfg['lifters_spreadsheet_key'], cfg['lifters_sheet_name'], col_names=True, credentials=credentials)

# fill in missing values
lifters['skip_opl'] = lifters['skip_opl'].fillna(False)
lifters['skip_opl'] = lifters['skip_opl'].astype(bool)

# validate lifters
for index, lifter in lifters.iterrows():
    validate = validate_lifter(lifter)
    if validate != True:
        print(f"Invalid lifter: {lifter['id']}, error: {validate}")
        lifters.drop(index, inplace=True)

# change the dates
lifters['matricDate']=lifters['matricDate'].apply(toDate)
lifters['gradDate']=lifters['gradDate'].apply(toDate)

# get results from openpowerlifting.org
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

for index, lifter in lifters.iterrows():
    if not lifter['skip_opl']:
        df = pd.read_csv(f"https://www.openpowerlifting.org/api/liftercsv/{lifter['id']}")

        # drop equipped comps
        df.drop(df[df.Equipment!='Raw'].index, inplace=True)

        # rename columns
        df.rename(mapper=rename_map, axis=1, inplace=True)

        # add lifter id and status
        df['lifter_id'] = lifter['id']
        df['id']=df['lifter_id']+df['meetDate']
        
        # convert dates
        df['meetDate'] = df['meetDate'].apply(toDate)
        df['status'] = df.apply(lambda x: get_status(x['meetDate'], lifter['matricDate'], lifter['gradDate']), axis=1)
    
        df.drop(columns=drop_columns, inplace=True)
        frames.append(df)

opl_results = pd.concat(frames)

# get results from manual entries
manual_results = g2d.download(cfg['records_spreadsheet_key'], cfg['manual_sheet_name'], col_names=True, credentials=credentials)

# fill in missing values
manual_results['squat'] = manual_results['squat'].fillna(0)
manual_results['bench'] = manual_results['bench'].fillna(0)
manual_results['deadlift'] = manual_results['deadlift'].fillna(0)
manual_results['total'] = manual_results['total'].fillna(0)
manual_results['bodyweight'] = manual_results['bodyweight'].fillna(0)
manual_results['squat'] = manual_results['squat'].astype(float)
manual_results['bench'] = manual_results['bench'].astype(float)
manual_results['deadlift'] = manual_results['deadlift'].astype(float)
manual_results['total'] = manual_results['total'].astype(float)
manual_results['bodyweight'] = manual_results['bodyweight'].astype(float)

# validate
for index, row in manual_results.iterrows():
    x = validate_manual_result(row, lifters)
    if x != True:
        print(f"Invalid manual result: {row['id']}, error: {x}")
        manual_results.drop(index, inplace=True)

# change dates
manual_results['meetDate'] = manual_results['meetDate'].apply(toDate)

# for each row in manual results, use the lifter id to get the grad and matric dates
manual_results['matricDate'] = manual_results.apply(lambda x: lifters[lifters['id']==x['lifter_id']]['matricDate'].values[0], axis=1)
manual_results['gradDate'] = manual_results.apply(lambda x: lifters[lifters['id']==x['lifter_id']]['gradDate'].values[0], axis=1)

# add status
manual_results['status'] = manual_results.apply(lambda x: get_status(x['meetDate'], x['matricDate'], x['gradDate']), axis=1)

# combine results
results = pd.concat([opl_results, manual_results])

# join results and lifters
data = results.merge(lifters, how="left", left_on="lifter_id", right_on="id")

# set up weight classes and categories
female_classes = cfg['female_classes']
male_classes = cfg['male_classes']
lifts = ['squat', 'bench', 'deadlift', 'total']

def class_boundaries_to_classes(boundaries):
    number_of_classes = len(boundaries)
    classes = [
        {'name': str(boundaries[0])+'kg', 'lower': 0, 'upper': boundaries[0]},
    ]
    for i in range(1,number_of_classes):
        classes.append({
            'name': str(boundaries[i])+'kg',
            'lower': boundaries[i-1],
            'upper': boundaries[i]
        })
    classes.append({
        'name': str(boundaries[-1])+'kg+',
        'lower': boundaries[-1],
        'upper': 999
    })
    return classes

female_classes = class_boundaries_to_classes(female_classes)
male_classes = class_boundaries_to_classes(male_classes)

sexes = ['M', 'F']
classes = {'M': male_classes, 'F':female_classes}
statuses = ['student', 'alumni']

# generate records

record_dump = []
record_log = []

record_columns = ['sex', 'status', 'weightclass', 'lift', 'fullName', 'liftKg', 'date']
old_records  = pd.read_csv('records.csv', names=record_columns)

for status in statuses:
    for sex in sexes:
        for weight_class in classes[sex]:

                valid_results = data[
                    (data['sex']==sex) &
                    (data['status']==status) &
                    (weight_class['lower'] < data['bodyweight']) &
                    (data['bodyweight'] <= weight_class['upper'])
                ]

                for lift in lifts:
                    
                    # get old record    
                    old = old_records[
                        (old_records['sex']==sex) &
                        (old_records['status']==status) &
                        (old_records['weightclass']==weight_class['name']) &
                        (old_records['lift']==lift)]
                    
                    oldKg = 0
                    for index,row in old.iterrows():
                        oldKg = float(row['liftKg'])

                    # compute new record
                    maxes = valid_results[valid_results[lift]==valid_results[lift].max()]
                    earliest = maxes[maxes['meetDate']==maxes['meetDate'].min()]
                    for index,row in earliest.iterrows():
                        record = [sex, status, weight_class['name'], lift, row['fullName'], row[lift], row['meetDate']]
                        record_dump.append(record)
                        if row[lift] > float(oldKg):
                            record_log.append(f"{row['meetDate'].strftime('%d/%m/%Y')}: New {status} {sex}{weight_class['name']} {lift} record of {row[lift]}kg (+{row[lift]-oldKg}kg) by {row['fullName']} at {row['meetName']}")

# dump record and logs to file
records=pd.DataFrame(data=record_dump, columns=record_columns)
records.to_csv('records.csv')
with open('record_log.txt', 'a') as f:
    for item in record_log:
        f.write("%s\n" % item)


# render records

tables = []
render_columns = ['class', 's_lifter', 's_record', 's_year', 'b_lifter', 'b_record', 'b_year', 'd_lifter', 'd_record', 'd_year', 't_lifter', 't_record', 't_year']
for status in statuses:
    for sex in sexes:
        table = []
        for weight_class in classes[sex]:
            row =  [weight_class['name']]
            for lift in lifts:

                record = records[
                    (records['sex']==sex) &
                    (records['status']==status) &
                    (records['weightclass']==weight_class['name']) &
                    (records['lift']==lift)]

                if record.shape[0] == 1:
                    subrow = [record.iloc[0]['fullName'], str(record.iloc[0]['liftKg'])+'kg', record.iloc[0]['date'].strftime("%d/%m/%Y")]
                else:
                    subrow = ['', '', '']
                row = row + subrow
            table.append(row)
        tables.append(pd.DataFrame(data=table, columns=render_columns))


# dump tables to gsheet


records_spreadsheet_key = cfg['records_spreadsheet_key']

#   student
wks_name = cfg['student_sheet_name']

#       men
d2g.upload(tables[0], records_spreadsheet_key, wks_name, credentials=credentials, start_cell='A4', clean=False, col_names=False, row_names=False)  

#       women
d2g.upload(tables[1], records_spreadsheet_key, wks_name, credentials=credentials, start_cell='A16', clean=False, col_names=False, row_names=False)      

#   alumni
wks_name = cfg['alumni_sheet_name']

#       men
d2g.upload(tables[2], records_spreadsheet_key, wks_name, credentials=credentials, start_cell='A4', clean=False, col_names=False, row_names=False)  

#       women
d2g.upload(tables[3], records_spreadsheet_key, wks_name, credentials=credentials, start_cell='A16', clean=False, col_names=False, row_names=False)    

print(f"{datetime.now()}: Succesfully updated with {len(record_log)} new records")

# logs

full_record_log = pd.read_csv('record_log.txt', header=None)
# split by the colon
full_record_log = full_record_log[0].str.split(':', n=1, expand=True)
# convert dates to datetime
full_record_log[0] = pd.to_datetime(full_record_log[0], format='%d/%m/%Y')
# sort by date first, then by name
full_record_log = full_record_log.sort_values(by=[0,1])
# convert back to one column with a :
full_record_log = full_record_log[0].astype(str) + ': ' + full_record_log[1]
# reverse order
full_record_log = full_record_log[::-1]
# convert to pandas dataframe
full_record_log = pd.DataFrame(full_record_log)
wks_name = cfg['log_sheet_name']
d2g.upload(full_record_log, records_spreadsheet_key, wks_name, credentials=credentials, start_cell='A2', clean=False, col_names=False, row_names=False)    
