from pydriller.git import Git 
import os
from pydriller.metrics.process.commits_count import CommitsCount
from pydriller.metrics.process.lines_count import LinesCount
from datetime import datetime
import copy 

def compute_repo_statistics(repo_path) :
    def compute_loc_per_file(repo_path,first_date = datetime.fromisoformat('2010-03-05 00:00:00.000000'),last_date=datetime.fromisoformat('2021-04-27 23:59:59.999999')): 
        metric = LinesCount(path_to_repo=repo_path,since=first_date,to=last_date)
        added = metric.count_added() 
        removed = metric.count_removed()
        current_loc = copy.deepcopy(added)
        for file_name in removed: 
            current_loc[file_name] -= removed[file_name]
        return current_loc

    def all_commits_per_file(repo_path,first_date = datetime.fromisoformat('2010-03-05 00:00:00.000000'),last_date=datetime.fromisoformat('2021-04-27 23:59:59.999999')): 
        metric = CommitsCount(path_to_repo=repo_path,since = first_date,to=last_date)
        files = metric.count()
        return files
    all_statistics = {'commits_count' : 0,'files_count' : 0 ,'total_loc':0}
    
    statistics = {'commits_count' : 0,'files_count' : 0 ,'total_loc':0}
    try:
        loc_per_file = compute_loc_per_file(repo_path,first_date = datetime.fromisoformat('2010-01-01 00:00:00.000000'),last_date=datetime.fromisoformat('2021-04-27 23:59:59.999999'))
        for file_name,loc in loc_per_file.items():
            statistics['total_loc'] += loc
        statistics['files_count'] = max(statistics['files_count'],len(loc_per_file))
        commits_per_file = all_commits_per_file(repo_path=repo_path,first_date = datetime.fromisoformat('2010-01-01 00:00:00.000000'),last_date=datetime.fromisoformat('2021-04-27 23:59:59.999999'))
        for file_name,commits_count in commits_per_file.items():
            statistics['commits_count'] += commits_count
        statistics['files_count'] = max(statistics['files_count'],len(loc_per_file))
        print(repo_path,'done')
        return statistics
    except:
        print('problem with: ', repo_path)
        return  statistics
   
   