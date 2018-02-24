"""
    Server API
    ~~~~~~~~~~

"""

from sqlite3 import dbapi2 as sqlite3
import flask
import uuid

bp = flask.Blueprint('ServerApi1', __name__)
main_db = None
rowver_db = None
synctable_db = None
main_curs = None
rowver_curs = None
synctable_curs = None


def error_manager(error_id, error_location, error_message):
    s = "{ERROR} [%i] [%s] : %s" % (error_id, error_location, error_message)
    print(s)


def info_manager(info_location, info_message)
    s = "{INFO} [%s] : %s" % (info_location, info_message)
    print(s)


def connect_main_db():
    global main_db
    global main_curs

    sqlite_file = "/home/stinksync/db/main_db.sqlite"
    main_db = sqlite3.connect(sqlite_file)
    main_db.row_factory = sqlite3.Row
    main_curs = main_db.cursor()


def connect_rowver_db():
    global rowver_db
    global rowver_curs

    sqlite_file = "/home/stinksync/db/rowver_db.sqlite"
    rowver_db = sqlite3.connect(sqlite_file)
    rowver_db.row_factory = sqlite3.Row
    rowver_curs = rowver_db.cursor()


def connect_synctable_db():
    global synctable_db
    global synctable_curs

    sqlite_file = "/home/stinksync/db/synctables_db.sqlite"
    synctable_db = sqlite3.connect(sqlite_file)
    synctable_db.row_factory = sqlite3.Row
    synctable_curs = synctable_db.cursor()


def init_synctable_db(curs):
    try:
        curs.execute("Begin Transaction")

        sql = "Create Table default_tables(uuid text, table_name text);"
        curs.execute(sql)


        curs.execute("End Transaction")
    except sqlite3.Error:
        error_manager(0, "init_synctable_db()", "Error while creating default table.")
        curs.close()


def create_uuid():
    return uuid.uuid4()

def create_init_script():
    global main_db
    global main_curs
    global synctable_db
    global synctable_curs

    # Get all tables to be synced from sync database
    sql = "Select * From sqlite_master Where type='table' And name <> 'merge_delete';"
    sync_rows = synctable_curs.execute(sql)
    sync_tbls = []
    for row in sync_rows:
        sync_tbls.append(row["name"])

    # Get all tables from database
    main_rows = main_curs.execute(sql)
    main_tbls = []
    for row in main_rows:
        main_tbls.append(row["name"])

    # Compare the 2 lists to make sure they match
    for tbl in sync_tbls:
        try:
            main_tbls.index(tbl)
        except RuntimeError:
            err = "Sync Failed. Could not find sync table %s in main database." % tbl
            error_manager(0, "create_init_script", err)
            return err

    # Loop through each
    for tbl in sync_tbls:
        # Get columns from table
        sql = "Pragma table_info(%s);" % tbl
        
        # Get column types from table
        # Add merge_update and merge_insert
        # Make Create Table Statement
        # Make Insert Trigger
        # Make Update Trigger
        # Make Delete Trigger

    # Compile to json


@bp.route('/')
def root_route():
    val = ""
    if main_db is not None and synctable_db is not None and rowver_db is not None:
        val = ""
    else:
        val = " not"
    return "StinkSync is Working! Database is%s connected" % val


@bp.route('/InitializeSubscriber/string:<sub_id>', methods=['Get'])
def init_subscriber_route(sub_id):
    global rowver_curs
    global synctable_curs
    global rowver_db
    global synctable_db

    # Create table for subscriber in rowver
    try:
        sql = "Create Table %s(uuid text Primary Key,row_id text,row_version text,table_name text);" % sub_id
        rowver_curs.execute(sql)
    except sqlite3.Error
        err = "Could not initialize subscriber %s. Error creating table in row version database." % sub_id
        error_manager(0, "init_subscriber_route", err)
        rowver_db.rollback()
        return  err

    # Create table for subscriber in synctable
    try:
        sql = "Create Table %s(uuid text Primary Key,table_name text);" % sub_id
        rowver_curs.execute(sql)
    except sqlite3.Error:
        err = "Could not initialize subscriber %s. Error creating table in sync tables database." % sub_id
        error_manager(0, "init_subscriber_route", err)
        rowver_db.rollback()
        return  err


    # Create script to build local db


    # Send script to subscriber


@bp.route('/AddTable/string:<table_name>', methods=['Get'])
def add_table_route(table_name):
    global main_curs
    global synctable_curs
    global main_db
    global synctable_db

    # Check that the table exists
    sql = "Select * From sqlite_master Where type = 'table' And name =?"
    vals = [(1, table_name)]

    main_curs.execute(sql, vals)
    row = main_curs.fetchone()

    if row is None:
        # table does not exist
        error_manager(0, "add_table_rout", "Error adding table %s. Table does not exist." % table_name)
        error_message = "Error adding table %s. Table does not exist." % table_name
        main_db.rollback()
        synctable_db.rollback()
        return error_message

    try:
        # Add table to default sync list
        new_uuid = create_uuid()
        sql = "Insert Into default_tables(uuid, table_name) Values(?, ?);"
        vals = ([1, new_uuid], [2, table_name])

        synctable_curs.execute(sql, vals)
    except sqlite3.Error:
        error_manager(0, "add_table_route", "Error adding table %s. Insert Error Encountered" % table_name)
        error_message = "Error adding table %s. Insert Error Encountered" % table_name
        main_db.rollback()
        synctable_db.rollback()
        return error_message

    # Check if the tracking fields exist
    sql = "Pragma table_info(%s);" % table_name
    main_curs.execute(sql)
    rows = main_curs.fetchmany()

    columns_exist = False
    for row in rows:
        if row["name"] == "row_version":
            columns_exist = True
            exit()

    if not columns_exist:

        try:
            # Add tracking fields to main db for this table
            sql = "Alter Table %s Add Column row_version text;"
            vals = ([1, table_name])
            main_curs.execute(sql, vals)
        except sqlite3.Error:
            error_message = "Error adding table %s. Could not add row version field." % table_name
            error_manager(0, "add_table_route", error_message)
            main_db.rollback()
            synctable_db.rollback()
            return error_message

    # Commit everything
    main_db.commit()
    synctable_db.commit()

    # Return Results to subscriber
    return "Table %s added successfully" % table_name


@bp.route('/RemoveTable/string:<table_name>', methods=['Get'])
def remove_table(table_name):
    global main_curs
    global synctable_curs
    global main_db
    global synctable_db

    # Check that the table exists
    sql = "Select * From sqlite_master Where type = 'table' And name =?"
    vals = [(1, table_name)]

    main_curs.execute(sql, vals)
    row = main_curs.fetchone()

    if row is None:
        # table does not exist
        error_message = "Error removing table %s. Table does not exist." % table_name
        error_manager(0, "remove_table_route", error_message)
        main_db.rollback()
        synctable_db.rollback()
        return error_message

    try:
        # Add table to default sync list
        sql = "Delete From default_tables Where table_name = ?;"
        vals = ([2, table_name])

        synctable_curs.execute(sql, vals)
    except sqlite3.Error:
        error_message = "Error removing table %s. Delete Error Encountered" % table_name
        error_manager(0, "remove_table_route", error_message)
        main_db.rollback()
        synctable_db.rollback()
        return error_message

    # Check if the tracking fields exist
    sql = "Pragma table_info(%s);" % table_name
    main_curs.execute(sql)
    rows = main_curs.fetchmany()

    columns_exist = False
    for row in rows:
        if row["name"] == "row_version":
            columns_exist = True
            exit()

    if columns_exist:

        try:
            # Add tracking fields to main db for this table
            sql = "Update %s Set row_version = '';"
            vals = ([1, table_name])
            main_curs.execute(sql, vals)
        except sqlite3.Error:
            error_message = "Error removing table %s. Could not remove row version field." % table_name
            error_manager(0, "add_table_route", error_message)

    # Commit everything
    main_db.commit()
    synctable_db.commit()

    # Return Results to subscriber
    return "Table %s removed successfully" % table_name



@bp.route('/Sync/uuid:<sub_id>/string:<table_name>/', methods=['Get'])
def sync_route(sub_id, table_name):
    pass


@bp.route('/CommitSync/int:<sync_id>', methods=['Get'])
def commit_sync_route(sync_id):
    pass

