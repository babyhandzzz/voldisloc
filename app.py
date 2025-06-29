from flask import Flask, jsonify
import os
import threading
import traceback
import utilities.load_historical_options_data as loader

app = Flask(__name__)

@app.route("/", methods=["GET"])
def index():
    return jsonify({"status": "ok", "message": "Voldisloc service is running. Use /run to start data loading."})

@app.route("/run", methods=["POST", "GET"])
def run_loader():
    print("/run endpoint called. Spawning background thread...")
    def run_in_background():
        print("[Background] Entered run_in_background.")
        try:
            print("[Background] About to call loader.main()...")
            loader.main()
            print("[Background] loader.main() finished.")
        except Exception as e:
            print(f"[Background] Exception: {e}")
            traceback.print_exc()
        print("[Background] Exiting run_in_background.")
    thread = threading.Thread(target=run_in_background)
    thread.start()
    return jsonify({"status": "started"}), 202

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
#