# s3 bucket

BUCKET_NAME = 'mlops-ucu-2024'
DATA_KEY = 'popular_movies.csv'
MODEL_KEY = 'lr_model.pkl'
REGION_NAME = 'eu-north-1'

# artifacts

LAST_RUN_ID = '23bb9943e5f64060bd58227810fa4343'
MODEL_FOLDER_NAME = 'letterboxd-predictions'
ARTIFACTS_FOLDER = f'mlflow-artifacts/{LAST_RUN_ID}/artifacts'
ENCODERS_FOLDER_PATH =f'{ARTIFACTS_FOLDER}/encoders'
MODEL_PATH = f'{ARTIFACTS_FOLDER}/{MODEL_FOLDER_NAME}/model.pkl'