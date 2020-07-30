import sqlite3
from . import app
from flask import render_template, g, request, redirect, url_for

DATABASE = './habromir.sqlite3'

def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
    return db

@app.route('/')
def index():
    cur = get_db().cursor()
    res = cur.execute("SELECT * FROM habr ORDER BY ID DESC LIMIT 30")
    last30 = res.fetchall()
    return render_template("index.html", last30 = last30)

@app.route('/', methods=['POST'])
def index_post():
    text = request.form['url']
    text_id = ''.join(x for x in text if x.isdigit())
    return redirect(url_for('show_post', id = text_id))

@app.route('/post/<int:id>/', methods=['POST'])
def index_post2(id):
    text = request.form['url']
    text_id = ''.join(x for x in text if x.isdigit())
    return redirect(url_for('show_post', id = text_id))


@app.route('/post/<int:id>/')
def show_post(id):
    cur = get_db().cursor()
    res = cur.execute("SELECT * FROM habr WHERE id = ?", (id,))
    post = res.fetchone()
    if post is None:
        return render_template("404.html", id = "Error 404")
    date = ', '.join(post[1].rstrip('+00:00').split('T'))
    return render_template("post.html", post = post, id = post[0], date = date)


