from flask import Flask, request, jsonify
import boto3
import pickle
import numpy as np
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

from properties.local_properties import REGION_NAME, BUCKET_NAME, MODEL_KEY
from properties.secrets import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

app = Flask(__name__)


def load_model_from_s3(bucket, key):
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=REGION_NAME
    )
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        serialized_model = response['Body'].read()
        model = pickle.loads(serialized_model)
        return model
    except (NoCredentialsError, PartialCredentialsError) as e:
        print("Credentials not available", e)
        return None
    except Exception as e:
        print("Error occurred while fetching the model from S3", e)
        return None


model = load_model_from_s3(BUCKET_NAME, MODEL_KEY)


@app.route('/predict', methods=['POST'])
def predict():
    if not model:
        return jsonify({'error': 'Model not loaded'}), 500

    data = request.json
    required_fields = ['Year', 'Views', 'Likes', 'Minutes']

    if not all(field in data for field in required_fields):
        return jsonify({'error': 'Invalid input data'}), 400

    try:
        input_data = np.array([
            data['Year'],
            data['Views'],
            data['Likes'],
            data['Minutes']
        ]).reshape(1, -1)

        prediction = model.predict(input_data)
        formatted_prediction = float(f"{prediction[0]:.1f}")
        return jsonify({'predicted_rating': formatted_prediction})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
