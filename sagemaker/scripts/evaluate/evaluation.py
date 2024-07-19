import json
import os
import tarfile
import joblib
import pathlib
import mlflow
import logging
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

logging.getLogger("mlflow").setLevel(logging.DEBUG)

encoders_folder = 'encoders'
model_folder = 'models'
encoder_files_names_list=[
    'mlb_countries',
    'mlb_genres',
    'mlb_directors',
    'mlb_cast',
    'onehot_language',
]

if __name__ == "__main__":
    encoders_folder = "/opt/ml/processing/encoders"
    model_path = os.path.join("/opt/ml/processing/model", "model.tar.gz")
    print(f"Extracting model from path: {model_path}")

    with tarfile.open(model_path) as tar:
        tar.extractall(path=".")
    print("Loading model")
    model = joblib.load("model.joblib")

    print("Loading test input data...")
    test_features_data = os.path.join("/opt/ml/processing/test", "test_features.csv")
    test_labels_data = os.path.join("/opt/ml/processing/test", "test_labels.csv")

    X_test = pd.read_csv(test_features_data, header=None)
    y_test = pd.read_csv(test_labels_data, header=None)
    
    y_pred = model.predict(X_test)
    
    mae = mean_absolute_error(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    report_dict = {
        "mean_absolute_error": mae,
        "mean_squared_error": mse,
        "r2_score": r2
    }

    print(f"Classification report:\n{report_dict}")
    
    print('Writing data to MLflow')
    mlflow.set_tracking_uri(os.environ['MLFLOW_TRACKING_ARN'])
    
    experiment_name = os.environ['MLFLOW_EXPERIMENT_NAME']
    mlflow.set_experiment(experiment_name)
    print(f'MLflow experiment: {experiment_name}')
    signature = mlflow.models.infer_signature(X_test, y_pred)

    with mlflow.start_run(run_name=f'Run {type(model).__name__}') as run:
        print(f'Run id: {run.info.run_id}')
        model_info = mlflow.sklearn.log_model(
            model, 
            model_folder, 
            signature=signature, 
            input_example=X_test.head(1))
        mlflow.log_metric('R2', r2)
        mlflow.log_metric('MAE', mae)
        mlflow.log_metric('MSE', mse)
        
        for file_name in encoder_files_names_list:
            file_path = os.path.join(encoders_folder, f'{file_name}.pkl')
            if os.path.exists(file_path):
                mlflow.log_artifact(file_path, encoders_folder)
            else:
                print(f"Warning: Artifact {file_path} not found.")
        
        print('Successfully logged metrics and artifacts')
    
    output_dir = "/opt/ml/processing/evaluation"
    pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    evaluation_path = f"{output_dir}/evaluation.json"
    print(f"Saving classification report to {evaluation_path}")

    with open(evaluation_path, "w") as f:
        f.write(json.dumps(report_dict))
    
    print('Evaluation saved successfully')