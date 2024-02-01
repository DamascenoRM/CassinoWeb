from flask import Flask
import asyncio
import threading
from bin.system import maintainer, get_env_config
import logging


# Setup logger
logging.basicConfig(level=logging.WARN, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
get_env_config()
app = Flask(__name__)

async def run_maintainer():
    await maintainer()

# Run maintainer in a separate thread using asyncio
loop = asyncio.get_event_loop()
maintainer_thread = threading.Thread(target=lambda: loop.run_until_complete(run_maintainer()))
maintainer_thread.start()

@app.route('/')
def hello_world():
    return 'Hello World!'

if __name__ == '__main__':
    app.run()
