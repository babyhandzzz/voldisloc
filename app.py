from flask import Flask, jsonify
import os
import threading
import traceback
import sys
import utilities.load_historical_options_data as loader

app = Flask(__name__)

@app.route("/", methods=["GET"])
def index():
    print("[Root] / endpoint called.", file=sys.stdout, flush=True)
    return jsonify({"status": "ok", "message": "Voldisloc service is running. Use /run to start data loading."})

@app.route("/run", methods=["POST", "GET"])
def run_loader():
    print("/run endpoint called. Spawning background thread...", file=sys.stdout, flush=True)
    def run_in_background():
        print("[Background] Entered run_in_background.", file=sys.stdout, flush=True)
        try:
            print("[Background] About to call loader.main()...", file=sys.stdout, flush=True)
            loader.main()
            print("[Background] loader.main() finished.", file=sys.stdout, flush=True)
        except Exception as e:
            print(f"[Background] Exception: {e}", file=sys.stdout, flush=True)
            traceback.print_exc()
        print("[Background] Exiting run_in_background.", file=sys.stdout, flush=True)
    thread = threading.Thread(target=run_in_background)
    thread.start()
    return jsonify({"status": "started"}), 202

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
#