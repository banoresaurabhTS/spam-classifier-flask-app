def filter_special_characters(text):
    import re
    text = re.sub('[^a-zA-Z0-9]', ' ', text)
    text = ' '.join(text.split())
    return(text)
    
def read_stop_words():
    for word in open('stopwords.txt', 'r'):
        words = word.split(',')
        return(words)
    
def filter_stop_words(text):
    stop_words = read_stop_words()
    for word in stop_words:
        text = text.replace(' '+ word + ' ', ' ')
    return(text)

def process_text(text):
#     from htmllaundry import strip_markup
#     text = strip_markup(text)
    text = filter_special_characters(text)
    text = text.lower()
    text = filter_stop_words(text)
    text = text.strip()
    text = ' '.join(text.split())
    return(text)

def load_block_keyword_table():
    
    # Environment should have S3 Credentials
    import pyarrow.parquet as pq
    import s3fs
    import pandas as pd
    
    # s3 = s3fs.S3FileSystem()
    # s3_path = "s3://townscript-data-lake/eventsystemdatabase/"
    # block_keywords = pq.ParquetDataset(s3_path + 'dewa_event_block_event_keyword_table', filesystem = s3).read_pandas().to_pandas()
    
    # return(block_keywords)
    return pd.read_csv('csvs/block_keywords.csv')

def find_spam_score(text, text_type, keywords_dataframe):
    keywords = keywords_dataframe
    
    # text_type = 2 for Event Name (factor also becomes 2)
    # text_type = 1 for Event Description (factor also becomes 1)
    
    factor = text_type
    text = process_text(text)
    score = 0
    for index, row in keywords.iterrows():
        weight = row['WEIGHT']
        keyword = row['KEYWORD_NAME']
        word_score = 0
        if keyword in text:
            count = text.count(keyword)
            word_score = word_score + (count * weight) + factor
        score = score + word_score
    return(score)



