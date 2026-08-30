import subprocess
import sys
import signal

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def streaming():
    ohlcv_process = subprocess.Popen(
        [sys.executable, "ohlcv_stream.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )
    
    interbar_process = subprocess.Popen(
        [sys.executable, "interbar_stream.py"], 
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )

    processes = [("OHLCV", ohlcv_process), ("INTERBAR", interbar_process)]
    def exit(sig, frame):
        logger.info("Shutting down processor streams")
        for name, process in processes:
            if process.poll() is None:
                process.terminate()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, exit)
    signal.signal(signal.SIGTERM, exit)
    
    while True:
        completed = True
        for name, process in processes:
            if process.poll() is None:
                completed = False
                if process.stdout:
                    line = process.stdout.readline()
                    if line:
                        logger.info(f"{name} {line.strip()}")
            elif process.returncode != 0:
                print(f"{name} terminated with return code: {process.returncode}")
        if completed:
            break

if __name__ == "__main__":
    streaming()            