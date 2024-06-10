from flask import Flask, request, jsonify
import boto3
import pickle
import os
import pandas as pd
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from dotenv import load_dotenv

from properties import *

app = Flask(__name__)

load_dotenv()

def load_object_from_s3(key):
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
        region_name=REGION_NAME
    )
    try:
        response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        serialized_model = response['Body'].read()
        return pickle.loads(serialized_model)
    except (NoCredentialsError, PartialCredentialsError) as e:
        print("Credentials not available", e)
        return None
    except Exception as e:
        print("Error occurred while fetching the model from S3", e)
        return None


model = load_object_from_s3(MODEL_PATH)

mlb_countries = load_object_from_s3(f'{ENCODERS_FOLDER_PATH}/mlb_countries.pkl')
mlb_genres = load_object_from_s3(f'{ENCODERS_FOLDER_PATH}/mlb_genres.pkl')
mlb_directors = load_object_from_s3(f'{ENCODERS_FOLDER_PATH}/mlb_directors.pkl')
mlb_cast = load_object_from_s3(f'{ENCODERS_FOLDER_PATH}/mlb_cast.pkl')
onehot_language = load_object_from_s3(f'{ENCODERS_FOLDER_PATH}/onehot_language.pkl')

def preprocess_input(data):
    df = pd.DataFrame([data])
    
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

    # Ensure the encoded data has the same column order as the training data
    encoded_data = encoded_data.reindex(columns=model.feature_names_in_, fill_value=0)
    
    return encoded_data


@app.route('/predict', methods=['POST'])
def predict():
    if not model:
        return jsonify({'error': 'Model not loaded'}), 500

    data = request.json
    required_fields = ['Year', 'Minutes', 'Countries', 'Genres', 'Directors', 'Cast', 'Language']

    if not all(field in data for field in required_fields):
        return jsonify({'error': 'Invalid input data'}), 400

    try:
        preprocessed_data = preprocess_input(data)

        prediction = model.predict(preprocessed_data)
        formatted_prediction = float(f"{prediction[0]:.1f}")
        return jsonify({'predicted_rating': formatted_prediction})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
