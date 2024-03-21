import os

from flask import Flask, request, jsonify
from google.cloud import bigquery
from google.oauth2 import service_account

app = Flask(__name__)

current_directory = os.path.dirname(__file__)


service_account_json_str = os.environ.get("GCP_SERVICE_ACCOUNT_JSON")
# Parse the JSON string into a dictionary
service_account_info = json.loads(service_account_json_str)

credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)
client = bigquery.Client(credentials=credentials, project=credentials.project_id)


# ------------
# order_status
# ------------
@app.route('/pgc/api/v1/order_status/push_status', methods=['POST'])
def order_data():
    print("test123")
    try:
        project_id = "pgc-phretail-omni-trans-live"
        dataset_id = "omisell"
        table_id = "order_status_push"
        dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
        table_ref = dataset_ref.table(table_id)

        payload = request.get_json()

        print(payload)

        if not payload or not isinstance(payload, list):
            return jsonify({"error": "Invalid payload format"}), 400

        formatted_payload = [
            {
                "order_number": item["order_number"],
                "omisell_order_number": item["omisell_order_number"],
                "created_time": item["created_time"],
                "updated_time": item["updated_time"],
                "status_id": item["status_id"],
                "status_name": item["status_name"],
                "shop_id": item["shop_id"],
                "platform": item["platform"],
                "timestamp": item["timestamp"],
                "request_id": item["request_id"]
            }
            for item in payload
        ]

        response = client.insert_rows_json(table_ref, formatted_payload)

        #response = True

        if not response:
            return jsonify({"message": f"Rows inserted successfully"}), 200
        else:
            return jsonify({"error": f"Error inserting rows: {response}"}), 500

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)  # (port=5000)