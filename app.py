from flask import Flask, jsonify
import os
import utilities.load_historical_options_data as loader

app = Flask(__name__)

@app.route("/run", methods=["POST", "GET"])
def run_loader():
    try:
        # Call your main function or logic here
        loader.main()  # You may need to adjust this if the entry point is different
        return jsonify({"status": "success"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
