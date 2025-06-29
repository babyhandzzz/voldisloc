from flask import Flask, jsonify
import os
import threading
import utilities.load_historical_options_data as loader

app = Flask(__name__)

@app.route("/", methods=["GET"])
def index():
    return jsonify({"status": "ok", "message": "Voldisloc service is running. Use /run to start data loading."})

@app.route("/run", methods=["POST", "GET"])
def run_loader():
    def run_in_background():
        print("Starting loader.main()")
        try:
            loader.main()
        except Exception as e:
            print(f"Background error: {e}")
        print("Finished loader.main()")
    thread = threading.Thread(target=run_in_background)
    thread.start()
    return jsonify({"status": "started"}), 202

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)