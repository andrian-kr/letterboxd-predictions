import os
import json
import boto3
import pickle
import joblib
import pandas as pd
from preprocessing import Preprocessor

BUCKET_NAME = 'mlops-ucu-2024'
REGION_NAME = 'eu-north-1'
ENCODERS_FOLDER = f'encoders'

def load_object_from_s3(key):
    s3 = boto3.client('s3', region_name=REGION_NAME)
    response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    serialized_obj = response['Body'].read()
    return pickle.loads(serialized_obj)

mlb_countries = load_object_from_s3(f'{ENCODERS_FOLDER}/mlb_countries.pkl')
mlb_genres = load_object_from_s3(f'{ENCODERS_FOLDER}/mlb_genres.pkl')
mlb_directors = load_object_from_s3(f'{ENCODERS_FOLDER}/mlb_directors.pkl')
mlb_cast = load_object_from_s3(f'{ENCODERS_FOLDER}/mlb_cast.pkl')
onehot_language = load_object_from_s3(f'{ENCODERS_FOLDER}/onehot_language.pkl')

def preprocess(data):
    df = pd.DataFrame([data])=

    countries_encoded = mlb_countries.transform(df['Countries'])
    genres_encoded = mlb_genres.transform(df['Genres'])
    directors_encoded = mlb_directors.transform(df['Directors'])
    cast_encoded = mlb_cast.transform(df['Cast'])
    language_encoded = onehot_language.transform(df[['Language']])

    language_columns = onehot_language.get_feature_names_out()

    encoded_data = pd.concat([
        df[['Year', 'Minutes']],
        pd.DataFrame(countries_encoded, columns=mlb_countries.classes_),
        pd.DataFrame(genres_encoded, columns=mlb_genres.classes_),
        pd.DataFrame(directors_encoded, columns=mlb_directors.classes_),
        pd.DataFrame(cast_encoded, columns=mlb_cast.classes_),
        pd.DataFrame(language_encoded, columns=language_columns)
    ], axis=1)

    return encoded_data.values

def model_fn(model_dir):
    """
    Deserialize and return fitted model.
    """
    model_file = "model.joblib"
    print(model_dir)
    lr = joblib.load(open(os.path.join(model_dir, model_file), "rb"))
    return lr

def input_fn(request_body, request_content_type):
    if request_content_type == 'application/json':
        data = json.loads(request_body)
        preprocessed_data = preprocess(data)
        return preprocessed_data
    else:
        raise ValueError(f"Unsupported content type: {request_content_type}")

def predict_fn(input_data, model):
    prediction = model.predict(input_data)
    return prediction

def output_fn(prediction, response_content_type):
    if response_content_type == 'application/json':
        return json.dumps({'predicted_rating': float(f"{prediction[0]:.1f}")})
    else:
        raise ValueError(f"Unsupported response content type: {response_content_type}")
