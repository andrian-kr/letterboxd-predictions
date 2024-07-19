import argparse
import os
import warnings
import ast
import boto3

import pandas as pd
import numpy as np
import pickle
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MultiLabelBinarizer, OneHotEncoder

warnings.filterwarnings(action="ignore", category=UserWarning)

encoders_s3_uri='s3://mlops-ucu-2024/encoders/'

encoders_output_dir =  "/opt/ml/processing/encoders"
columns = ['Year', 'Rating', 'Minutes', 'Language','Genres','Countries','Directors','Cast']
non_encoded_columns=['Year', 'Minutes']

def get_top_n(elements_list, n=10):
    return pd.Series([item for sublist in elements_list for item in sublist]).value_counts().nlargest(n).index.tolist()

def encoder_to_file(s3, bucket_name, folder_path, encoder, filename):
    file_full_name = f'{filename}.pkl'
    local_path = os.path.join(encoders_output_dir, file_full_name)
    with open(local_path, 'wb') as f:
        pickle.dump(encoder, f)
    s3.upload_file(local_path, bucket_name, os.path.join(folder_path, file_full_name))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--train-test-split-ratio", type=float, default=0.1)
    parser.add_argument("--encoders-bucket-name", type=str, required=True)
    parser.add_argument("--encoders-folder", type=str, required=True)
    args, _ = parser.parse_known_args()

    print(f"Received arguments {args}")

    input_data_path = os.path.join("/opt/ml/processing/input", "popular_movies.csv")
    print(f'Reading input data from {input_data_path}')
    df = pd.read_csv(input_data_path)
    print(f'Input data shape: {df.shape}')
    
    print('Getting subset of data...')
    df_subset = df[columns].dropna()
    df_subset['Genres'] = df_subset['Genres'].apply(ast.literal_eval)
    df_subset['Countries'] = df_subset['Countries'].apply(ast.literal_eval)
    df_subset['Directors'] = df_subset['Directors'].apply(ast.literal_eval)
    df_subset['Cast'] = df_subset['Cast'].apply(ast.literal_eval)
    print(f'Subset data shape: {df_subset.shape}')
    
    X = df_subset.drop(columns=['Rating'])
    y = df_subset['Rating']

    split_ratio = args.train_test_split_ratio
    print(f"Splitting data into train and test sets with ratio {split_ratio}")
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=split_ratio, random_state=10)
    
    print('Getting values for encoding...')
    top_directors = get_top_n(df_subset['Directors'])
    print(f'Top directors: {top_directors}')
    top_cast = get_top_n(df_subset['Cast'])
    print(f'Top cast: {top_cast}')
    
    print(f'Encoding data...')
    mlb_countries = MultiLabelBinarizer()
    mlb_genres = MultiLabelBinarizer()
    mlb_directors = MultiLabelBinarizer(classes=top_directors)
    mlb_cast = MultiLabelBinarizer(classes=top_cast)
    onehot_language = OneHotEncoder(sparse_output=False,handle_unknown='ignore')
    
    print('Encoding train data...')
    countries_encoded_train = mlb_countries.fit_transform(X_train['Countries'])
    genres_encoded_train = mlb_genres.fit_transform(X_train['Genres'])
    directors_encoded_train = mlb_directors.fit_transform(X_train['Directors'])
    cast_encoded_train = mlb_cast.fit_transform(X_train['Cast'])
    language_encoded_train = onehot_language.fit_transform(X_train[['Language']])
    
    encoded_data_train = pd.concat([
        X_train[non_encoded_columns].reset_index(drop=True),
        pd.DataFrame(countries_encoded_train, columns=mlb_countries.classes_),
        pd.DataFrame(genres_encoded_train, columns=mlb_genres.classes_),
        pd.DataFrame(directors_encoded_train, columns=top_directors),
        pd.DataFrame(cast_encoded_train, columns=top_cast),
        pd.DataFrame(language_encoded_train, columns=onehot_language.get_feature_names_out())
    ], axis=1)
    print(f'Encoded train data shape: {encoded_data_train.shape}')
        
    print('Encoding test data...')
    countries_encoded_test = mlb_countries.transform(X_test['Countries'])
    genres_encoded_test = mlb_genres.transform(X_test['Genres'])
    directors_encoded_test = mlb_directors.transform(X_test['Directors'])
    cast_encoded_test = mlb_cast.transform(X_test['Cast'])
    language_encoded_test = onehot_language.transform(X_test[['Language']])
    
    encoded_data_test = pd.concat([
        X_test[non_encoded_columns].reset_index(drop=True),
        pd.DataFrame(countries_encoded_test, columns=mlb_countries.classes_),
        pd.DataFrame(genres_encoded_test, columns=mlb_genres.classes_),
        pd.DataFrame(directors_encoded_test, columns=top_directors),
        pd.DataFrame(cast_encoded_test, columns=top_cast),
        pd.DataFrame(language_encoded_test, columns=onehot_language.get_feature_names_out())
    ], axis=1)
    print(f'Encoded test data shape: {encoded_data_test.shape}')

    train_features_path = os.path.join("/opt/ml/processing/train", "train_features.csv")
    train_labels_output_path = os.path.join("/opt/ml/processing/train", "train_labels.csv")

    test_features_path = os.path.join("/opt/ml/processing/test", "test_features.csv")
    test_labels_output_path = os.path.join("/opt/ml/processing/test", "test_labels.csv")

    print(f"Saving training data to {train_features_path}")
    encoded_data_train.to_csv(train_features_path, index=False, header=False)

    print(f"Saving test data to {test_features_path}")
    encoded_data_test.to_csv(test_features_path, index=False, header=False)

    print(f"Saving training labels to {train_labels_output_path}")
    y_train.to_csv(train_labels_output_path, index=False, header=False)

    print(f"Saving test labels to {test_labels_output_path}")
    y_test.to_csv(test_labels_output_path, index=False, header=False)
    
    s3=boto3.client('s3')
    
    encoders_bucket_name = args.encoders_bucket_name
    encoders_folder = args.encoders_folder
    
    encoders = [
        (mlb_countries, 'mlb_countries'),
        (mlb_genres, 'mlb_genres'),
        (mlb_directors, 'mlb_directors'),
        (mlb_cast, 'mlb_cast'),
        (onehot_language, 'onehot_language')
    ]
    
    for encoder, name in encoders:
        encoder_to_file(s3, encoders_bucket_name, encoders_folder, encoder, name)