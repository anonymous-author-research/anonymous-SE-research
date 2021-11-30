import pandas as pd
import json
import os
from pydriller import RepositoryMining
from nltk import word_tokenize
from datetime import datetime
from multiprocessing import Pool

DATA_PATH = 'C:/Users/AQ38570/Desktop/work/code review and refactoring/data'

def compute_statistics(project_name = 'Openstack',url = 'https://review.opendev.org',considered_status = ['merged-changes','abondaned-changes'],keywords= {'refactoring' : ['refactor']},files=None):
    def compute_duration(change_data) : 
        update_time = datetime.fromisoformat(change_data['updated'][:-3])
        create_time = datetime.fromisoformat(change_data['created'][:-3])
        duration = update_time - create_time
        return duration.total_seconds()*1.0/3600
    
    def compute_code_churn(change_data) : 
        #chrun for the first revision
        first_revision_data = extract_revision(change_data,n_revision = 1)
        if first_revision_data!= None :
            first_revision_data_files =  first_revision_data['data']['files']
            churn = 0 
            for filename in first_revision_data_files : 
                churn += safe_get(first_revision_data_files[filename],'lines_deleted',0)
                churn += safe_get(first_revision_data_files[filename],'lines_inserted',0)
    
            return churn 
        else :
            return -1

        return change_data['deletions'] + change_data['insertions']
    def compute_reviewers_number(change_data) : 
        reviewers = safe_get(safe_get(change_data,'reviewers',{}),'REVIEWER',[])
        nb_reviewers = 0 
        for reviewer in reviewers : 
            if 'email' in reviewer : 
                nb_reviewers += 1 
        return nb_reviewers 

    def compute_exchanged_messages(change_data) :
        nb_messages = 0 
        discussion_length = 0 
        for message in change_data['messages'] : 
            if 'email' in safe_get(message,'author',{})  : 
                nb_messages += 1 
                discussion_length += len(message['message'])
            else :
                if 'email' in safe_get(message,'real_author',{})  : 
                    nb_messages += 1 
                    discussion_length += len(message['message'])
        return nb_messages, discussion_length

    def compute_nb_revisions(change_data) : 
        return len(change_data['revisions'])

    def compute_modified_files_number(change_data) : 
        return len(extract_first_revision(change_data)['data']['files'])

    def keywrods_count(keywords,text) : 
        splitted_text = [token.lower() for token in tokenize_text(text) ]
        kwds_count = {cat : {word : 0 for word in  keywords[cat]} for cat in keywords}
        found_cats = ''
        found_kwds = ''
        for cat in keywords : 
            found_category = False 
            for keyword in keywords[cat] : 
                if keyword.lower() in splitted_text : 
                    kwds_count[cat][keyword] += 1 
                    if not (cat in found_cats) : 
                        found_category = True 
                        found_cats += cat+'/'
                    if len(found_kwds) > 0 : 
                        found_kwds +=',' +  keyword 
                    else :
                        found_kwds +=   keyword
            if found_category :
                found_kwds +='|'
        return kwds_count,found_cats,found_kwds 

    def update_count(global_count,update) : 
        for cat in global_count : 
            for keyword in global_count[cat] : 
                global_count[cat][keyword] += update[cat][keyword]
        return global_count

    def read_keywords(kwd_path) : 
        return json.load(kwd_path)
    
    def extract_change_subject(change_data) :
        return change_data["subject"]

    def extract_change_description(change_data,repos_path) : 
        first_revision = extract_first_revision(change_data)['data']
        return safe_get(safe_get(first_revision,'commit',{}),'message','')
        
    def extract_revision(change_data,n_revision = 1) :
        for revision_id in  change_data['revisions'] :
            if change_data['revisions'][revision_id]['_number'] == n_revision: 
                return {'commit' : revision_id, 'data' :change_data['revisions'][revision_id] }
        return None 

    
    def extract_first_revision(change_data) : 
        revisions = safe_get(change_data,'revisions',{})
        result = {}
        for revision_id,revision_data in revisions.items() : 
            creation_date = datetime.fromisoformat(revision_data['created'][:-3])
            if len(result) == 0 : 
                result = {'commit' : revision_id, 'data' : revision_data}
            else : 
                if (datetime.fromisoformat(result['data']['created'][:-3]) > creation_date) : 
                    result = {'commit' : revision_id, 'data' : revision_data}
        return result 
    
    def extract_change_url(change_data,url) : 
        change_number = change_data['_number']
        return url+  '/'  + str(change_number)
    
    def tokenize_text(text) : 
        return word_tokenize(text)
    
    def clean_text(text) : 
        result = text
        result = result.replace('\n',' ')
        result = result.strip()
        return result 
    def extract_revision_data(change_data,revision_id) : 
        revisions = safe_get(change_data,'revisions',{})
        return safe_get(revisions,revision_id,{})

    def safe_get(data,key,default=None) : 
        try:
            if key in data: 
                return data[key]
            return default
        except : 
            print(data) 
            
    
    def extract_repo_name(change_data) : 
        project_name = change_data['project'].replace('/','--')
        return project_name
    
    subject_keywords_count = {cat : {word : 0 for word in  keywords[cat]} for cat in keywords}
    description_keywords_count = {cat : {word : 0 for word in  keywords[cat]} for cat in keywords}
    result =[]# pd.DataFrame(columns = ['id','#reviewers','#messages','churn','#revisions','#files',
             #                        'duration','len_description','description_wordcount',
             #                        'subject categories','subject keywords','description categories',
              #                       'description keywords'])
    global_changes_count = 0 
    for current_status in considered_status : 
        print('current status:',current_status)
        data_path = os.path.join(DATA_PATH,project_name,'raw data','review data',current_status)
        if files == None : 
            loop_files = os.listdir(data_path)
        else : 
            loop_files = files[current_status]
        for  filename in loop_files:
            if '.json' in filename : 
                print('processing file:',filename)
                raw_data = json.load(open(os.path.join(data_path,filename), encoding="utf8"))
                for change_data in raw_data : 
                    global_changes_count += 1 
                    change_subject = extract_change_subject(change_data)
                    change_description = clean_text(extract_change_description(change_data,os.path.join(DATA_PATH,project_name,'raw data','git repos')))
                    subject_kwds_count,subject_found_cats,subject_found_kwds  = keywrods_count(keywords,change_subject)
                    if change_description != None :
                        desc_kwds_count,desc_found_cats,desc_found_kwds  = keywrods_count(keywords,change_description)
                    else : 
                        change_description = ''
                        desc_kwds_count = {cat : {word : 0 for word in  keywords[cat]} for cat in keywords}
                        desc_found_cats = ''
                        desc_found_kwds = ''
                    if len(subject_found_kwds) > 0 or len(desc_found_kwds) > 0:
                        print('bingo')
                        subject_keywords_count = update_count(subject_keywords_count,subject_kwds_count)
                        description_keywords_count = update_count(description_keywords_count,desc_kwds_count)
                    nb_messages, discussion_length = compute_exchanged_messages(change_data)
                    new_row = {
                            'id': change_data['id'],
                            'url' : extract_change_url(change_data,url),
                            'branch': change_data['branch'],                            
                            '#reviewers' : compute_reviewers_number(change_data),
                            '#messages' : nb_messages,
                            '#inline_comments' : safe_get(change_data,'total_comment_count',0),
                            '#unsolved_inline_comments' : safe_get(change_data,'unresolved_comment_count',0),
                            'len_messages' :discussion_length, 
                            'churn' : compute_code_churn(change_data),
                            '#revisions' : compute_nb_revisions(change_data),
                            '#files' :  compute_modified_files_number(change_data),
                            'duration' : compute_duration(change_data),
                            'len_description': len(change_description),
                            'subject categories' : subject_found_cats,
                            'subject keywords' : subject_found_kwds,
                            'description categories' :desc_found_cats,
                            'description keywords' : desc_found_kwds,
                            'subject' : change_subject, 
                            'description' : change_description
                        }
                    result.append(new_row)

    return result,subject_keywords_count, description_keywords_count,global_changes_count





