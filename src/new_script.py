from dataclasses import dataclass
from argparse import ArgumentParser
import logging
import json
import gspread
import df2gspread.df2gspread as d2g
import df2gspread.gspread2df as g2d
from oauth2client.service_account import ServiceAccountCredentials
import datetime
import pandas as pd

OPL_RENAME_MAP = {
    'BodyweightKg': 'bodyweight',
    'Best3SquatKg': 'squat',
    'Best3BenchKg': 'bench',
    'Best3DeadliftKg': 'deadlift',
    'TotalKg': 'total',
    'MeetName': 'meetName',
    'Date': 'meetDate'
}

OPL_RESULTS_COLUMNS = ['id', 'lifter_id', 'student', 'alumni', 'meetName', 'meetDate', 'bodyweight', 'squat', 'bench', 'deadlift', 'total']

SEXES = ['M', 'F']
STATUSES = ['student', 'alumni']

@dataclass
class Config:
    service_account: str
    lifters_spreadsheet_key: str
    lifters_sheet_name: str
    female_classes: list[int]
    male_classes: list[int]
    records_spreadsheet_key: str
    manual_sheet_name: str
    student_sheet_name: str
    alumni_sheet_name: str
    log_sheet_name: str

def load_config(config_path: str) -> Config:
    """Load config from a json file path"""
    logger = logging.getLogger(__name__)
    logging.info("Loading config from %s", config_path)
    with open(config_path, "r") as f:
        config = json.load(f)
    config = Config(**config)
    logging.info("Config loaded")
    return config

    
def get_google_service_account_credentials(cfg: Config) -> ServiceAccountCredentials:
    """Get google service account credentials from a json file path"""
    logger = logging.getLogger(__name__)
    logging.info("Getting google service account credentials")
    scope = ['https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(cfg.service_account, scope)
    gspread.authorize(credentials)
    logging.info("Google service account credentials loaded")
    return credentials

def toDate(date_string: str):
    """Converts a date string in the format YYYY-MM-DD to a datetime object"""
    return datetime.strptime(date_string,'%Y-%m-%d')

@dataclass
class WeightClass:
    name: str
    sex: str
    lower: float
    upper: float

def build_weight_classes(cfg:Config) -> list[WeightClass]:
    male_boundaries = cfg.male_classes
    female_boundaries = cfg.female_classes 
    classes = []
    for sex, boundaries in zip(["M, F"], [male_boundaries, female_boundaries]):
        classes.append(
            WeightClass(str(boundaries[0]+'kg'), sex, 0, boundaries[0]))
        for i in range(1,len(boundaries)):
            classes.append(
                WeightClass(str(boundaries[i]+'kg'), sex, boundaries[i-1], boundaries[i])
            )
        classes.append(
            WeightClass(boundaries[-1]+'kg+', sex, boundaries[-1], 999)
        )
    return classes

class Lifters:
    def __init__(self, cfg: Config, credentials: ServiceAccountCredentials):
        self.cfg = cfg
        self.credentials = credentials
        self.logger = logging.getLogger(__name__)
        self.load_lifters_from_drive()
        self.clean_data()
        
    def load_lifters_from_drive(self):
        self.data = g2d.download(self.cfg.lifters_spreadsheet_key, self.cfg.lifters_sheet_name, col_names=True, credentials=self.credentials)

    def clean_data(self):
        self.data['matricDate']=self.data['matricDate'].apply(toDate)
        self.data['gradDate']=self.data['gradDate'].apply(toDate)
        self.data['skip_opl'] = self.data['skip_opl'].fillna(False)
        self.data['skip_opl'] = self.data['skip_opl'].astype(bool)
        
class Results:
    def __init__(self, cfg: Config, credentials:ServiceAccountCredentials, lifters: Lifters, weight_classes: list[WeightClass]):
        self.cfg = cfg
        self.lifters = lifters
        self.credentials = credentials
        self.logger = logging.getLogger(__name__)
        self.weight_classes = weight_classes
        self.load_results_from_opl()
        self.load_manual_results()
        self.data = pd.concat([self.opl_results, self.manual_results])

    def get_status(self, meetDate, matricDate, gradDate):
        if matricDate <= meetDate <= gradDate:
            return 'student'
        elif gradDate < meetDate:
            return 'alumni'
        return 'none'
    
    def get_class(self, bodyweight: float, sex: str):
        for weight_class in self.weight_classes:
            if sex == weight_class.sex and weight_class.lower < bodyweight <= weight_class.upper:
                return weight_class.name
    
    def load_results_from_opl(self):
        frames = []
        for index, lifter in self.lifters.data.iterrows():
            if lifter['skip_opl']:
                continue
            
            df = pd.read_csv(f"https://www.openpowerlifting.org/u/{lifter['id']}/csv")

            # drop equipped comps
            df.drop(df[df.Equipment!='Raw'].index, inplace=True)

            # rename columns
            df.rename(mapper=OPL_RENAME_MAP, axis=1, inplace=True)
            
            # drop all columns not in OPL_NAME_MAP.keys()
            df.drop(columns=[col for col in df.columns if col not in OPL_RESULTS_COLUMNS], inplace=True)

            # add ids
            df['lifter_id'] = lifter['id']
            df['id']=df['lifter_id']+df['meetDate']
            
            # convert dates and get status and weight class
            df['meetDate'] = df['meetDate'].apply(toDate)
            df['status'] = df.apply(lambda x: self.get_status(x['meetDate'], lifter['matricDate'], lifter['gradDate']), axis=1)
            df['class'] = df.apply(lambda x: self.get_class(x['bodyweight'], lifter['sex']), axis=1)
            
            frames.append(df)

        self.opl_results = pd.concat(frames)
        
    def load_manual_results(self):
        # get results from manual entries
        manual_results = g2d.download(self.cfg.records_spreadsheet_key, self.cfg.manual_sheet_name, col_names=True, credentials=self.credentials)
        manual_results['meetDate'] = manual_results['meetDate'].apply(toDate)

        # cast manual results 
        manual_results['squat'] = manual_results['squat'].astype(float)
        manual_results['bench'] = manual_results['bench'].astype(float)
        manual_results['deadlift'] = manual_results['deadlift'].astype(float)
        manual_results['total'] = manual_results['total'].astype(float)
        manual_results['bodyweight'] = manual_results['bodyweight'].astype(float)

        # for each row in manual results, use the lifter id to get the grad and matric dates
        manual_results['matricDate'] = manual_results.apply(lambda x: self.lifters.data[self.lifters.data['id']==x['lifter_id']]['matricDate'].values[0], axis=1)
        manual_results['gradDate'] = manual_results.apply(lambda x: self.lifters.data[self.lifters.data['id']==x['lifter_id']]['gradDate'].values[0], axis=1)

        # add status
        manual_results['status'] = manual_results.apply(lambda x: self.get_status(x['meetDate'], x['matricDate'], x['gradDate']), axis=1)
        manual_results['class'] = manual_results.apply(lambda x: self.get_class(x['bodyweight'], self.lifters.data[self.lifters.data['id']==x['lifter_id']]['sex'].values[0], axis=1))
    
class Records:
    def __init__(self, cfg: Config, credentials:ServiceAccountCredentials, lifters: Lifters, results: Results, weight_classes: list[WeightClass]):
        self.cfg = cfg
        self.lifters = lifters
        self.results = results
        self.credentials = credentials
        self.weight_classes = weight_classes

        
        self.data = self.results.data.merge(results.lifters.data, how="left", left_on="lifter_id", right_on="id")
        self.record_columns = ['sex', 'status', 'weightclass', 'lift', 'fullName', 'liftKg', 'date']
        self.statuses = ["student", "alumni"]
        self.lifts = ["squat", "bench", "deadlift", "total"]
        self.instantiate_records_df()
        self.compute_records()
        self.load_old_records()
        self.compute_diff()
        
    def instantiate_records_df(self):
        self.new_records = pd.DataFrame(columns=self.record_columns)
        
    def compute_records(self):
        for status in self.statuses:
            for weight_class in self.weight_classes:
                sex = weight_class.sex
                valid_results = self.data[(self.data['status']==status) & (self.data['class']==weight_class.name) & (self.data['sex'] == sex)]
                for lift in self.lifts:
                    maxes = valid_results[valid_results[lift]==valid_results[lift].max()]
                    earliest = maxes[maxes['meetDate']==maxes['meetDate'].min()]
                    for index,row in earliest.iterrows():
                        record = [sex, status, weight_class['name'], lift, row['fullName'], row[lift], row['meetDate']]
                        self.new_records.loc[len(self.new_records)] = record
    
    def load_old_records(self):
        self.old_records = pd.read_csv("data/records.csv")
    
    def compute_record_diff
                
        
        

           
  
    
      
def main():
    argparser = ArgumentParser()
    argparser.add_argument("--config", type=str, default="config.json")
    args = argparser.parse_args()
    
    cfg = load_config(args.config)
    
    credentials = get_google_service_account_credentials(cfg)
    
    
    weight_classes = build_weight_classes(cfg)
    lifters = Lifters(cfg, credentials)
    results = Results(cfg, credentials, lifters)
    records = Records(cfg, credentials, lifters, results)
    
    
    
    

if __name__ == "__main__":
    main()