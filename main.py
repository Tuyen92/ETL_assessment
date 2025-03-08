from datetime import datetime, timedelta
from dotenv import load_dotenv
from fastapi import FastAPI, Query, HTTPException
from typing import Optional
import os
import pymysql
import requests

load_dotenv()
app = FastAPI()

# REPO
REPO_OWNER = os.getenv('REPO_OWNER')
REPO_NAME = os.getenv('REPO_NAME')

# GITHUB API
BASE_URL = os.getenv('BASE_URL')
TOKEN = os.getenv('TOKEN')

# DATABASE CONNECTION, USING MYSQL WORKBENCH
DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
DB_PORT = int(os.getenv('DB_PORT'))

DB_CONFIG = {
    "host": DB_HOST,
    "user": DB_USER,
    "password": DB_PASSWORD,
    "database": DB_NAME,
    "port": DB_PORT
}

def connect_db():
    return pymysql.connect(**DB_CONFIG)

@app.get("/")
def home():
    return "Welcome"

@app.get("/commits")
def get_commits():
    # calculate the time of the since 6 month
    time = (datetime.utcnow() - timedelta(days=180)).isoformat()
    data_length = 100
    page = 1

    # connect to the database and create cursor to execute the SQL
    conn = connect_db()
    cursor = conn.cursor()

    while data_length == 100:
        # run github endpoint to extract data
        url = f'{BASE_URL}/repos/{REPO_OWNER}/{REPO_NAME}/commits?since={time}&per_page=100&page={page}'
        headers = {"Authorization": f"token {TOKEN}"}
        response = requests.get(url, headers = headers)
        commits = response.json()
        data_length = len(commits)

        if (data_length == 0):
            break

        # handle json response to store the data
        for commit in commits:
            sha = commit["sha"]
            author = None
            if "author" in commit and commit["author"] and "login" in commit["author"]:
                author = commit["author"]["login"]
            committer = commit["committer"]["id"]
            message = commit["commit"]["message"]
            commit_datetime = commit["commit"]["author"]["date"]

            # change the datetime format for MySQL
            format_commit_datetime = datetime.strptime(commit_datetime, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")

            cursor.execute("SELECT COUNT(*) FROM commits WHERE sha = %s", (sha))
            if cursor.fetchone()[0] == 0:
                cursor.execute("INSERT INTO commits (sha, committer, message, date, author) VALUES (%s, %s, %s, %s, %s)", (sha, committer, message, format_commit_datetime, author))
            else:
                cursor.execute("UPDATE commits SET committer = %s, message = %s, date = %s, author = %s WHERE sha = %s", (committer, message, format_commit_datetime, author, sha))

        page += 1
        # save change in the database
        conn.commit()

    # close the cursor and the database connection
    cursor.close()
    conn.close()

    return commits

@app.get("/committers")
def get_top_committers(top: Optional[int] = Query(5, ge=1)):
    conn = connect_db()
    cursor = conn.cursor()

    try:
        # select all record and group by author for counting number of commit
        cursor.execute('''
            SELECT author, COUNT(*) AS number_of_commits
            FROM commits
            GROUP BY author
            ORDER BY number_of_commits DESC
            LIMIT %s;''', (top))
        results = cursor.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        # close the cursor and the database connection
        cursor.close()
        conn.close()

    return results

@app.get("/committers/streak")
def get_streak():
    conn = connect_db()
    cursor = conn.cursor()

    try:
        # 1. select authors with date of commit
        # 2. calculate the streak: check if the previous commit date and date are continuously and the same author the increase streak by one, if not set streak to 1
        # 3. remember the maximum of streak
        # 4. select the authors with highest streak
        cursor.execute('''
            SELECT author, streak
            FROM (
                SELECT
                    author,
                    commit_date,
                    @previous_commit_date AS previous_commit_date,
                    @previous_author AS previous_author,
                    @streak := IF(@previous_author = author AND DATEDIFF(commit_date, @previous_commit_date) = 1, @streak + 1, 1) AS streak,
                    @max_streak := GREATEST(@max_streak, @streak) AS max_streak,
                    @previous_commit_date := commit_date,
                    @previous_author := author
                FROM (
                    SELECT DISTINCT author, DATE(date) AS commit_date
                    FROM etl_assessment.commits
                    ORDER BY author, commit_date
                    ) AS ordered_commits,
                    (SELECT @previous_commit_date := NULL, @previous_author := NULL, @streak := 0, @max_streak := 0) AS variables
            ) AS streak_commit
            WHERE streak = @max_streak
            ORDER BY author;
        ''')
        results = cursor.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        # close the cursor and the database connection
        cursor.close()
        conn.close()

    return results

@app.get('/commits/heatmap')
def generate_heatmap():
    conn = connect_db()
    cursor = conn.cursor()

    try:
        # go through each record and check the hour, if hour's in one case then increase the total commit by one
        cursor.execute('''
            SELECT
                DAYNAME(date) AS day_of_week,
                SUM(CASE WHEN HOUR(date) BETWEEN 1 AND 3 THEN 1 ELSE 0 END) AS '01-03',
                SUM(CASE WHEN HOUR(date) BETWEEN 4 AND 6 THEN 1 ELSE 0 END) AS '04-06',
                SUM(CASE WHEN HOUR(date) BETWEEN 7 AND 9 THEN 1 ELSE 0 END) AS '07-09',
                SUM(CASE WHEN HOUR(date) BETWEEN 10 AND 12 THEN 1 ELSE 0 END) AS '10-12',
                SUM(CASE WHEN HOUR(date) BETWEEN 13 AND 15 THEN 1 ELSE 0 END) AS '13-15',
                SUM(CASE WHEN HOUR(date) BETWEEN 16 AND 18 THEN 1 ELSE 0 END) AS '16-18',
                SUM(CASE WHEN HOUR(date) BETWEEN 19 AND 21 THEN 1 ELSE 0 END) AS '19-21',
                SUM(CASE WHEN HOUR(date) >= 22 OR HOUR(date) = 0 THEN 1 ELSE 0 END) AS '22-00'
            FROM commits
            GROUP BY day_of_week
            ORDER BY FIELD(day_of_week, 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday');
        ''')
        results = cursor.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        # close the cursor and the database connection
        cursor.close()
        conn.close()

    return results