from flask import Flask
from threading import Thread

app = Flask('')

@app.route('/')
def main():
  return "Your Bot Is Ready"

def run():
  try:
    app.run(host="0.0.0.0", port=8000)
  except:pass

def keep_alive():
  server = Thread(target=run)
  server.start()