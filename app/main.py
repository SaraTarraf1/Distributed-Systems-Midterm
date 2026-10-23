
from flask import Flask, jsonify
from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST

app = Flask(__name__)

REQUEST_COUNT = Counter(
    "midterm_requests_total",
    "Total number of demo requests"
)

@app.route("/health")
def health():
    return jsonify(status="healthy", service="midterm-api")

@app.route("/demo")
def demo():
    REQUEST_COUNT.inc()
    return jsonify(message="Distributed Systems Midterm is working!")

@app.route("/metrics")
def metrics():
    return generate_latest(), 200, {
        "Content-Type": CONTENT_TYPE_LATEST
    }

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
