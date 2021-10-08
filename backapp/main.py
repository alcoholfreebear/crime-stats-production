
# from fastapi import FastAPI
from flask import Flask
from operations import *

# app=FastAPI() #fastapi alternative
app=Flask(__name__)

@app.route("/raw")
def run_main():
    try:
        main()
        return({"status":"success!"})
    except:
        return({"status":"failure!"})

@app.route("/translate")
def run_translate():
    try:
        translate_ops()
        return({"status":"success!"})
    except:
        return({"status":"failure!"})

if __name__=='__main__':
    import sys
    if len(sys.argv)>1:
        if sys.argv[1]=='translate':
            translate_ops()
        else:
            print(f"Option {sys.argv[1]} doesn't exist. Enter option 'translate' if applicable." )
    else:
        main()
