"""AgentCore Runtime HTTP server: /ping + /invocations (forwards to kiro-cli)."""
from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import subprocess


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/ping":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "Healthy", "agent": "kiro"}).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        if self.path == "/invocations":
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length).decode() if content_length else "{}"
            try:
                payload = json.loads(body)
            except json.JSONDecodeError:
                payload = {}

            prompt = payload.get("prompt", "hello")
            try:
                result = subprocess.run(
                    ["/app/run.sh", "chat", prompt],
                    capture_output=True, text=True, timeout=300,
                    env={"PATH": "/home/agent/.local/bin:/usr/local/bin:/usr/bin:/bin",
                         "HOME": "/home/agent",
                         "AWS_REGION": "us-east-1",
                         "AWS_DEFAULT_REGION": "us-east-1"},
                )
                output = result.stdout or result.stderr or "No output"
            except subprocess.TimeoutExpired:
                output = "Error: timed out after 300s"
            except Exception as e:
                output = f"Error: {e}"

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"response": output}).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass


if __name__ == "__main__":
    print("AgentCore Kiro server on :8080")
    HTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
