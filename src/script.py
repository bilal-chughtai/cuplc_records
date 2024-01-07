from dataclasses import dataclass
from argparse import ArgumentParser
import logging
import logging.handlers
import json
import gspread
import df2gspread.df2gspread as d2g
import df2gspread.gspread2df as g2d
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import pandas as pd
import os
import pickle
import sys
from typing import Optional

OPL_RENAME_MAP = {
    'BodyweightKg': 'bodyweight',
    'Best3SquatKg': 'squat',
    'Best3BenchKg': 'bench',
    'Best3DeadliftKg': 'deadlift',
    'TotalKg': 'total',
    'MeetName': 'meetName',
    'Date': 'meetDate'
}

SEXES = ['M', 'F']
STATUSES = ['student', 'alumni']
LIFTS = ['squat', 'bench', 'deadlift', 'total']


""" Config and Credentials"""

@dataclass
class Config:
    service_account: str
    mail_credentials: str
    lifters_spreadsheet_key: str
    lifters_sheet_name: str
    female_weightclasses: list[int]
    male_weightclasses: list[int]
    records_spreadsheet_key: str
    manual_sheet_name: str
    student_sheet_name: str
    alumni_sheet_name: str
    log_sheet_name: str

def get_google_service_account_credentials(cfg: Config) -> ServiceAccountCredentials:
    """Get google service account credentials from a json file path"""
    logger = logging.getLogger(__name__)
    try:
        scope = ['https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive']
        credentials = ServiceAccountCredentials.from_json_keyfile_name(cfg.service_account, scope)
        gspread.authorize(credentials)
        return credentials
    except Exception as e:
        logger.error(f"Fatal error authorising with Google Drive.", exc_info=True)
        raise e


def load_config(config_path: str) -> Config:
    """Load config from a json file path"""
    logger = logging.getLogger(__name__)
    logging.info("Loading config from %s", config_path)
    with open(config_path, "r") as f:
        config = json.load(f)
    config = Config(**config)
    logging.info("Config loaded")
    return config

def get_mail_credentials(credential_path:str) -> tuple[str, str]:
    """Get email credentials from a json file path"""
    logger = logging.getLogger(__name__)
    logging.info("Getting email credentials")
    with open(credential_path, "r") as f:
        credentials = json.load(f)
    credentials = (credentials["username"], credentials["password"])
    logging.info("Email credentials loaded")
    return credentials


""" Weight Classes"""

@dataclass
class WeightClass:
    name: str
    sex: str
    lower: float
    upper: float

def build_weight_classes(cfg:Config) -> list[WeightClass]:
    male_boundaries = cfg.male_weightclasses
    female_boundaries = cfg.female_weightclasses
    classes = []
    for sex, boundaries in zip(["M", "F"], [male_boundaries, female_boundaries]):
        classes.append(
            WeightClass(str(boundaries[0])+'kg', sex, 0, boundaries[0]))
        for i in range(1,len(boundaries)):
            classes.append(
                WeightClass(str(boundaries[i])+'kg', sex, boundaries[i-1], boundaries[i])
            )
        classes.append(
            WeightClass(str(boundaries[-1])+'kg+', sex, boundaries[-1], 999)
        )
    return classes

""" Lifters """

@dataclass 
class Lifter:
    id: str
    fullName: str
    sex: str
    matricDate: datetime
    gradDate: datetime
    skip_opl: bool
    

def toDate(date_string: str):
    """Converts a date string in the format YYYY-MM-DD to a datetime object"""
    return datetime.strptime(date_string,'%Y-%m-%d')


def get_lifters(cfg: Config, credentials: ServiceAccountCredentials) -> dict[str, Lifter]:
    logger = logging.getLogger(__name__)

    try:
        data = g2d.download(cfg.lifters_spreadsheet_key, cfg.lifters_sheet_name, col_names=True, credentials=credentials)
    except Exception as e:
        logger.error(f"Fatal error loading all lifter data from Google Drive.", exc_info=True)
        raise e

    lifters = {}
    for index, row in data.iterrows():
        try:
            row['matricDate'] = toDate(row['matricDate'])
            row['gradDate'] = toDate(row['gradDate'])
            row['skip_opl'] = row['skip_opl'] if pd.notna(row['skip_opl']) else False
            row['skip_opl'] = bool(row['skip_opl'])
            lifter = Lifter(**row)
            lifters[lifter.id] = lifter
        except Exception as e:
            # Log the error and skip the row
            logger.error(f"Error initialising lifter {row['fullName']}. Skipping lifter.", exc_info=True)
            continue

    return lifters

""" Results """

@dataclass
class Result:
    id: str
    lifter_id: str
    status: str
    meetName: str
    meetDate: datetime
    bodyweight: float
    squat: float
    bench: float
    deadlift: float
    total: float
    weightclass: WeightClass
    
    
def get_status(meetDate: datetime, matricDate:datetime, gradDate:datetime) -> str:
    if matricDate <= meetDate <= gradDate:
        return 'student'
    elif gradDate < meetDate:
        return 'alumni'
    return 'none'

def get_weightclass(bodyweight: float, sex: str, weightclasses: list[WeightClass]) -> WeightClass:
    for weightclass in weightclasses:
        if sex == weightclass.sex and weightclass.lower < float(bodyweight) and float(bodyweight) <= weightclass.upper:
            return weightclass
            

def load_opl_results(lifters: dict[str, Lifter], weight_classes: list[WeightClass]) -> list[Result]:
    logger = logging.getLogger(__name__)
    results = []

    for lifter in lifters.values():
        if lifter.skip_opl:
            continue
        
        try:
            df = pd.read_csv(f"https://www.openpowerlifting.org/api/liftercsv/{lifter.id}")
        except:
            logger.error(f"Error loading results from Open Powerlifting for lifter {lifter.fullName} via url https://www.openpowerlifting.org/api/liftercsv/{lifter.id}. Skipping lifter.", exc_info=True)
            continue
        
        # rename columns to match Result dataclass
        df.rename(mapper=OPL_RENAME_MAP, axis=1, inplace=True)
        for i, row in df.iterrows():
            
            if row['Equipment'] != 'Raw':
                # Skip equipped results
                continue

            try:
                meetDate = toDate(row['meetDate'])
                squat = float(row['squat']) if pd.notna(row['squat']) else 0
                bench = float(row['bench']) if pd.notna(row['bench']) else 0
                deadlift = float(row['deadlift']) if pd.notna(row['deadlift']) else 0
                total = float(row['total']) if pd.notna(row['total']) else 0
                result = Result(
                    id = lifter.id + row['meetDate'],
                    lifter_id = lifter.id,
                    status = get_status(meetDate, lifter.matricDate, lifter.gradDate),
                    meetName = row['meetName'],
                    meetDate = meetDate,
                    bodyweight = float(row['bodyweight']),
                    squat = squat, 
                    bench = bench,
                    deadlift = deadlift,
                    total = total,
                    weightclass=get_weightclass(row["bodyweight"], lifter.sex, weight_classes)
                )
                results.append(result)
            except Exception as e:
                # Log the error and skip the row
                logger.error(f"Error processing Open Powerlifting result {row['id']}. Skipping result. ", exc_info=True)
                continue
            
    return results

        
def load_manual_results(cfg: Config, credentials: ServiceAccountCredentials, lifters: dict[str, Lifter], weightclasses: list[WeightClass]) -> list[Result]:
    logger = logging.getLogger(__name__)
    
    df = g2d.download(cfg.records_spreadsheet_key, cfg.manual_sheet_name, col_names=True, credentials=credentials)    
    results = []
    
    for i, row in df.iterrows():

        try:
            meetDate = toDate(row['meetDate'])
            lifter = lifters.get(row['lifter_id'], None)
            if lifter is None:
                logger.error(f"Could not find lifter for manual result {row['id']} with id {row['lifter_id']}")
                continue
            
            result = Result(
                id = row['id'],
                lifter_id = row['lifter_id'],
                status = get_status(meetDate, lifter.matricDate, lifter.gradDate),
                meetName = row['meetName'],
                meetDate = meetDate,
                bodyweight = float(row['bodyweight']),
                squat = float(row['squat']),
                bench = float(row['bench']),
                deadlift = float(row['deadlift']),
                total = float(row['total']),
                weightclass = get_weightclass(float(row["bodyweight"]), lifter.sex, weightclasses)
            )
            results.append(result)
        except Exception as e:
            # Log the error and skip the row
            logger.error(f"Error processing manual result with id {row['id']}. Skipping result.", exc_info=True)
            continue
    return results

""" Records """

@dataclass
class Record:
    sex: str
    status: str
    weightclass: WeightClass
    lift: str
    fullName: str
    liftKg: float
    date: datetime
    meetName: str


def compute_records(lifters: dict[str, Lifter], results: list[Result], weightclasses: list[WeightClass]) -> list[Record]:
    records = []
    for status in STATUSES:
        for weightclass in weightclasses:
            sex = weightclass.sex
            valid_results = [result for result in results if result.status == status and result.weightclass == weightclass]
            for lift in LIFTS:
                valid_kgs = [getattr(result, lift) for result in valid_results]
                if len(valid_kgs) == 0:
                    continue
                record_kg = max(getattr(result, lift) for result in valid_results)
                max_lift_results = [result for result in valid_results if getattr(result, lift) == record_kg]
                min_date = min(result.meetDate for result in max_lift_results)
                earliest_max_lift_results = [result for result in max_lift_results if result.meetDate == min_date]
                record_result = earliest_max_lift_results[0]
                lifter = lifters[record_result.lifter_id]
                record = Record(
                    sex = lifter.sex,
                    status = status,
                    weightclass = weightclass,
                    lift = lift,
                    fullName = lifter.fullName,
                    liftKg = getattr(record_result, lift),
                    date = record_result.meetDate,
                    meetName = record_result.meetName
                )
                records.append(record)
    return records

""" Log and Export """
                     
def save_to_file(records: list[Record]) -> None:
    # pickle records
    with open('data/records.pickle', 'wb') as f:
        pickle.dump(records, f)

def load_records_from_file() -> Optional[list[Record]]:
    if os.path.exists('data/records.pickle'):
        with open('data/records.pickle', 'rb') as f:
            records = pickle.load(f)
        return records
    else:
        return None

def diff_records(new_records: list[Record], old_records: list[Record]) -> list[str]:
    log = []
    for new_record in new_records:
        # find corresponding old record
        for old_record in old_records:
            if old_record.sex == new_record.sex and old_record.status == new_record.status and old_record.weightclass == new_record.weightclass and old_record.lift == new_record.lift:
                if old_record.liftKg != new_record.liftKg:
                    # put it in this form: 2023-12-10:  New student F76kg total record of 395.0kg (+5.0kg) by Emmanuela Onah at ACE Performance Christmas Championships
                    log.append(f"New {new_record.status} {new_record.weightclass.sex}{new_record.weightclass.name} {new_record.lift} record of {new_record.liftKg}kg (+{new_record.liftKg - old_record.liftKg}kg) by {new_record.fullName} at {new_record.meetName}")
    return log
                    


def render_records(records: list[Record], weightclasses: list[WeightClass]) -> list[pd.DataFrame]:
    """ Returns a list of 4 dataframes, containining records in the order student male, student female, alumni male, alumni female"""
    tables = []
    render_columns = ['class', 's_lifter', 's_record', 's_year', 'b_lifter', 'b_record', 'b_year', 'd_lifter', 'd_record', 'd_year', 't_lifter', 't_record', 't_year']
    for status in STATUSES:
        for sex in SEXES:
            table = []
            for weightclass in weightclasses:
                if weightclass.sex == sex:
                    row = [weightclass.name]
                    for lift in LIFTS:
                        valid_records = [record for record in records if record.sex == sex and record.status == status and record.weightclass == weightclass and record.lift == lift] 
                        if len(valid_records) == 1:
                            current = valid_records[0]
                            subrow = [current.fullName, str(current.liftKg)+'kg', current.date.strftime("%d/%m/%Y")]
                        else:
                            subrow = ['', '', '']
                        row = row + subrow
                    table.append(row)
            tables.append(pd.DataFrame(data=table, columns=render_columns))
    return tables

def export_log_and_records(tables: list[pd.DataFrame], log: list[str], cfg: Config, credentials: ServiceAccountCredentials):
    
    # student men
    d2g.upload(tables[0], cfg.records_spreadsheet_key, cfg.student_sheet_name, credentials=credentials, start_cell='A4', clean=False, col_names=False, row_names=False)  

    # student women
    d2g.upload(tables[1], cfg.records_spreadsheet_key, cfg.student_sheet_name, credentials=credentials, start_cell='A16', clean=False, col_names=False, row_names=False)      

    # alumni men
    d2g.upload(tables[2], cfg.records_spreadsheet_key, cfg.alumni_sheet_name, credentials=credentials, start_cell='A4', clean=False, col_names=False, row_names=False)  

    # alumni women
    d2g.upload(tables[3], cfg.records_spreadsheet_key, cfg.alumni_sheet_name, credentials=credentials, start_cell='A16', clean=False, col_names=False, row_names=False)    

    # download record log from A2:A1000
    old_record_log = g2d.download(cfg.records_spreadsheet_key, cfg.log_sheet_name, col_names=False, credentials=credentials, start_cell='A2')
    # append new log to top of old log
    record_log = pd.DataFrame(log)
    record_log = pd.concat([record_log, old_record_log])
    d2g.upload(record_log, cfg.records_spreadsheet_key, cfg.log_sheet_name, credentials=credentials, start_cell='A2', clean=False, col_names=False, row_names=False)    

""" Main """

def main():
    
    logger = logging.getLogger('MyLogger')
    logger.setLevel(logging.ERROR)  # Only log errors and above
    argparser = ArgumentParser()
    argparser.add_argument("--config", type=str, default="src/config.json")
    args = argparser.parse_args()
    
    cfg = load_config(args.config)
    mail_credentials = get_mail_credentials(cfg.mail_credentials)
    mail_handler = logging.handlers.SMTPHandler(
        mailhost=("smtp.srcf.net", 587),  # Replace with your SMTP server and port
        fromaddr="cuplc-webmaster@srcf.net",  # Your email address
        toaddrs=["cuplc-webmaster@srcf.net"],  # Destination email address
        subject="Error in cuplc-records script",
        credentials=mail_credentials,  # Specify SMTP credentials
        secure=()  # Tuple for secure sending, leave empty for default
    )
    logger.addHandler(mail_handler)
    
    # catch any other exception
    try:
        credentials = get_google_service_account_credentials(cfg)
        weightclasses = build_weight_classes(cfg)
        
        lifters = get_lifters(cfg, credentials)
        opl_results = load_opl_results(lifters, weightclasses)
        manual_results = load_manual_results(cfg, credentials, lifters, weightclasses)
        all_results = opl_results + manual_results
        
        new_records = compute_records(lifters, all_results, weightclasses)
        old_records = load_records_from_file()
        
        if old_records is not None:
            log = diff_records(new_records, old_records)
        else:
            log = []
        tables = render_records(new_records, weightclasses)
        
        save_to_file(new_records)
        export_log_and_records(tables, log, cfg, credentials)
    except Exception as e:
        logging.error("Fatal unhandled exception occured :(", exc_info=True)  # Logs the traceback
        sys.exit(1)
    
if __name__ == "__main__":
    main()