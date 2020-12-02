# YACS

## Instructions to get started (locally)

Clone this repo:
```
git clone https://github.com/tfidfwastaken/yacs
cd yacs
```

A default `config.json` has been provided.

Open five terminals, and `cd` to `yacs` in all of them.  
Then run the following (assuming `python`  refers to python 3):

Terminal 1:
```
python yacs/master.py
```

Provide the scheduling algorithms at the prompt.

Terminal 2,3 and 4:
```
python yacs/worker.py <port number> <worker id>
```

Set the port number and worker ID according to the desired configuration in config.json.

Then finally for the last terminal, run:
```
python test/requests.py <n>
```
with `n` being the number of requests. You can alternatively use anything that sends valid request objects to port 5000 of master.
