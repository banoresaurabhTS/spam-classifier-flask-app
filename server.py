import pandas as pd
import requests
import spam_scorer
import boto3
from joblib import load
from datetime import date
import os.path
from flask import Flask, request, jsonify

app = Flask(__name__)

def load_data(event_data_dict):
    data =  pd.DataFrame([event_data_dict])
    print(data)
    return (data)
    
def find_spam_score(dataframe):
    dataframe = dataframe[['EVENT_NAME', 'EVENT_DESCRIPTION']]
    keywords = spam_scorer.load_block_keyword_table()
    score = (spam_scorer.find_spam_score(dataframe['EVENT_NAME'].iloc[0], 2, keywords)
             + spam_scorer.find_spam_score(dataframe['EVENT_DESCRIPTION'].iloc[0], 1, keywords))
    return(score)

def find_recent_file(bucket, file_path, conn):
    objects = conn.list_objects(Bucket = bucket, Prefix = file_path)['Contents']
    files_list = []
    for file in objects:
        file = file['Key'].split('/')[2]
        files_list.append(file)
    filename = files_list[len(files_list) - 1]
    return(filename)

def load_model(bucket, model_path, s3_resource):
    filename = find_recent_file(bucket, model_path, conn)
    file_path = os.path.join(model_path, filename)
    file_location = os.path.join('/tmp', filename)
    s3.meta.client.download_file(bucket, file_path, file_location)
    return(load(file_location))
    
def load_suspicious_conditions(bucket, suspicious_path, s3_object):
    filename = find_recent_file(bucket, suspicious_path, conn)
    file_path = os.path.join(suspicious_path, filename)
    file_location = os.path.join('/tmp', filename)
    s3.meta.client.download_file(bucket, file_path, file_location)
    suspicious = pd.DataFrame(pd.read_json(file_location, typ = 'series', orient = 'records')).transpose()
    return(suspicious)

def find_model_prediction(dataframe):
    clf = load_model(bucket, model_path, s3)
    dataframe = dataframe.drop(['EVENT_NAME', 'EVENT_DESCRIPTION'], axis = 1)
    prob = round(float(clf.predict_proba(dataframe)[:,1]),3)
    return(prob)

def is_suspicious(dataframe, spam_score):
    suspicious_conditions = load_suspicious_conditions(bucket, suspicious_path, s3)
    return (
        spam_score > 5
        or dataframe['EVENT_NAME_LENGTH'].iloc[0] < suspicious_conditions['EVENT_NAME_LENGTH'].iloc[0]
        or dataframe['EVENT_DESCRIPTION_LENGTH'].iloc[0] < suspicious_conditions['EVENT_DESCRIPTION_LENGTH_MIN'].iloc[0]
        or dataframe['EVENT_DESCRIPTION_LENGTH'].iloc[0] > suspicious_conditions['EVENT_DESCRIPTION_LENGTH_MAX'].iloc[0]
        or dataframe['HYPERLINKS_IN_EVENT_NAME'].iloc[0] > suspicious_conditions['HYPERLINKS_IN_EVENT_NAME'].iloc[0]
        or dataframe['EMAILS_IN_EVENT_NAME'].iloc[0] > suspicious_conditions['EMAILS_IN_EVENT_NAME'].iloc[0]
        or dataframe['PHONE_NUMBER_IN_EVENT_NAME'].iloc[0] > suspicious_conditions['PHONE_NUMBER_IN_EVENT_NAME'].iloc[0]
        or dataframe['HYPERLINKS_IN_EVENT_DESCRIPTION'].iloc[0] > suspicious_conditions['HYPERLINKS_IN_EVENT_DESCRIPTION'].iloc[0]
        or dataframe['EMAILS_IN_EVENT_DESCRIPTION'].iloc[0] > suspicious_conditions['EMAILS_IN_EVENT_DESCRIPTION'].iloc[0]
        or dataframe['PHONE_NUMBERS_IN_EVENT_DESCRIPTION'].iloc[0] > suspicious_conditions['PHONE_NUMBERS_IN_EVENT_DESCRIPTION'].iloc[0]
        or dataframe['EVENT_DURATION'].iloc[0] > suspicious_conditions['EVENT_DURATION'].iloc[0]
    )

# Implementing 

bucket = 'ts-data-pit'
model_path = 'spam-classifier/saved-models/'
suspicious_path = 'spam-classifier/suspicious-flag-conditions/'
conn = boto3.client('s3',use_ssl = False)
s3 = boto3.resource('s3')

@app.route("/classify", methods=['GET'])
def spam_classifier():
    event_data = request.json
    global dataframe
    dataframe = load_data(event_data)
    spam_score = find_spam_score(dataframe)
    data = [[spam_score, find_model_prediction(dataframe), is_suspicious(dataframe, spam_score), 'HAM']]
    columns = ['SPAM_SCORE', 'SPAM_PROB', 'SUSPICIOUS_FLAG', 'SPAM_STATUS']
    spam_result = pd.DataFrame(data = data, columns = columns)
    spam_result.loc[spam_result.SPAM_SCORE > 15, 'SPAM_STATUS'] = 'SPAM'
    spam_result.loc[(spam_result.SPAM_PROB > 0.5) &
                    (spam_result.SPAM_SCORE < 15), 'SPAM_STATUS'] = 'SPAM'
    spam_result.loc[(spam_result.SPAM_PROB < 0.5) &
                    (spam_result.SPAM_SCORE < 15) &
                    (spam_result.SUSPICIOUS_FLAG == True), 'SPAM_STATUS'] = 'SUSPICIOUS'
    spam_json = spam_result.drop('SUSPICIOUS_FLAG', axis = 1).to_json(orient = 'records')
    return(spam_json)

# def give_results_to_api():
    # TODO: Implementation for API Endpoint


if __name__ == "__main__":
    app.run(host='0.0.0.0')