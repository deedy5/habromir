from app import app
from waitress import serve

def main():
    serve(app, host='0.0.0.0', port=8080, threads=1) #WAITRESS!

if __name__ == '__main__':
    main()
