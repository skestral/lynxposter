from settings.paths import database_path, backup_path
from local.functions import write_log
import json, os, shutil, arrow


# Function for writing new lines to the database
def db_write(skeet, tweet, toot, discord, tumblr, bsky, failed, database):
    ids = {
        "twitter_id": tweet,
        "mastodon_id": toot,
        "discord_id": discord,
        "tumblr_id": tumblr,
        "bsky_id": bsky
    }
    data = {
        "ids": ids,
        "failed": failed
    }
    database[skeet] = data
    row = {
        "skeet": skeet,
        "ids": ids,
        "failed": failed
    }
    json_string = json.dumps(row)
    if os.path.exists(database_path):
        append_write = 'a'
    else:
        append_write = 'w'
    if not is_in_db(json_string):
        write_log("Adding to database: " + json_string)
        with open(database_path, append_write) as file:
            file.write(json_string + "\n")
    return database



# Function for reading database file and saving values in a dictionary
def db_read():
    database = {}
    if not os.path.exists(database_path):
        return database
    with open(database_path, 'r') as file:
        for line in file:
            try:
                json_line = json.loads(line)
            except:
                continue
            skeet = json_line["skeet"]
            ids = json_line["ids"]
            ids = db_convert(ids)
            failed = {"twitter": 0, "mastodon": 0, "discord": 0, "tumblr": 0}  # Adding tumblr failure count
            if "failed" in json_line:
                failed.update(json_line["failed"])  # Update with existing values, defaulting to 0 for missing keys
            line_data = {
                "ids": ids,
                "failed": failed
            }
            database[skeet] = line_data
    return database



# After changing from camelCase to snake_case, old database entries will have to be converted.
def db_convert(ids_in):
    ids_out = {}
    try:
        ids_out["twitter_id"] = ids_in["twitter_id"]
    except:
        ids_out["twitter_id"] = ids_in["twitterId"]
    try:
        ids_out["mastodon_id"] = ids_in["mastodon_id"]
    except:
        ids_out["mastodon_id"] = ids_in["mastodonId"]
    try:
        ids_out["discord_id"] = ids_in["discord_id"]
    except:
        ids_out["discord_id"] = ids_in.get("discordId", "")
    try:
        ids_out["tumblr_id"] = ids_in["tumblr_id"]
    except:
        ids_out["tumblr_id"] = ids_in.get("tumblrId", "")  # Adding conversion for tumblr_id
    return ids_out



# Function for checking if a line is already in the database-file
def is_in_db(line):
    if not os.path.exists(database_path):
        return False
    with open(database_path, 'r') as file:
        content = file.read()
        if line in content:
            return True
        else:
            return False


# Since we are working with a version of the database in memory, at the end of the run
# we completely overwrite the database on file with the one in memory.
# This does kind of make it unnecessary to write each new post to the file while running,
# but in case the program fails halfway through it gives us kind of a backup.
def save_db(database):
    write_log("Saving new database")
    append_write = "w"
    for skeet in database:
        row = {
            "skeet": skeet,
            "ids": database[skeet]["ids"],
            "failed": database[skeet]["failed"]
        }
        json_string = json.dumps(row)
        with open(database_path, append_write) as file:
            file.write(json_string + "\n")
        append_write = "a"


# Every twelve hours a backup of the database is saved, in case something happens to the live database.
# If the live database contains fewer lines than the backup it means something has probably gone wrong,
# and before the live database is saved as a backup, the current backup is saved as a new file, so that
# it can be recovered later.
def db_backup():
    # Ensure the backup directory exists
    backup_dir = os.path.dirname(backup_path)
    if not os.path.exists(backup_dir):
        os.makedirs(backup_dir)

    if not os.path.isfile(database_path) or (os.path.isfile(backup_path)
                                             and arrow.Arrow.fromtimestamp(
                os.stat(backup_path).st_mtime) > arrow.utcnow().shift(hours=-24)):
        return
    if os.path.isfile(backup_path):
        if count_lines(backup_path) < count_lines(database_path):
            os.remove(backup_path)
        else:
            date = arrow.utcnow().format("YYMMDD")
            os.rename(backup_path, backup_path + "_" + date)
            write_log("Current backup file contains more entries than current live database, backup saved", "error")
    shutil.copyfile(database_path, backup_path)
    write_log("Backup of database taken")


# Function for counting lines in a file
def count_lines(file):
    count = 0
    with open(file, 'r') as file:
        for count, line in enumerate(file):
            pass
    return count
