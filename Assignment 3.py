#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()
    cur.execute("Drop Table If Exists {} CASCADE".format(ratingstablename))
    cur.execute("Create Table {} (UserID INT,temp1 VARCHAR(10),MovieID INT, temp2 VARCHAR(10), Rating FLOAT,temp3 VARCHAR(10),Timestamp BIGINT)"
                .format(ratingstablename))
    file_data = open(ratingsfilepath, 'r')
    cur.copy_from(file_data, ratingstablename, sep=':', columns=('UserID', 'temp1','MovieID', 'temp2', 'Rating', 'temp3','Timestamp'))
    cur.execute("Alter Table {} Drop Column temp1, Drop Column temp2, Drop Column temp3, Drop Column Timestamp"
                .format(ratingstablename))

    openconnection.commit()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    #Range parition range_part
    cur = openconnection.cursor()
    table_name = 'range_part'
    starting_rate = 0
    i=0
    cur.execute(""" Create Table If Not Exists range_insert_table(part_num INT, starting_rate FLOAT, ending_rate FLOAT)""")
    cur.execute(""" Create Table If Not Exists range_insert_table_temp(part_num INT, starting_rate FLOAT, ending_rate FLOAT)""")

    while (i < numberofpartitions):
        temp = 5 / numberofpartitions
        ending_rate = starting_rate + temp
        range_name = table_name + str(i)
        cur.execute("Create Table If Not Exists {} (UserID INT, movieID INT, Rating FLOAT)".format(range_name))

        if (i == 0):
            cur.execute("Insert into {} select * from {a}  where {a}.rating >= {start} AND {a}.rating <= {end} "
                                      .format(range_name, a=ratingstablename, start = starting_rate, end = ending_rate))
        else:
            cur.execute("Insert into {} select * from {a}  where {a}.rating > {start} AND {a}.rating <= {end} "
                        .format(range_name, a = ratingstablename, start = starting_rate, end = ending_rate))

        #debug here??
        #cur.execute(""" CREATE TABLE range_insert_table """)
        cur.execute("Insert into range_insert_table Values ({},{},{})".format(i, starting_rate, ending_rate))
        cur.execute("Insert into range_insert_table_temp Values ({},{},{})".format(i, starting_rate, ending_rate))
        starting_rate = ending_rate
        i = i + 1
    deleteTables('range_insert_table_temp', openconnection)
    openconnection.commit()


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
        """cur = openconnection.cursor()
        table_name = 'rrobin_part'
        cur.execute("CREATE TABLE IF NOT EXISTS rrobin_temp (UserID INT, MovieID INT, Rating FLOAT, index INT)")
        cur.execute("Insert into rrobin_temp (SELECT {a}.UserID, {a}.MovieID, {a}.Rating , (ROW_NUMBER() OVER() -1) % {n} as index from {a})"
                    .format( a = ratingstablename, n = str(numberofpartitions)))
        index1 = 0
        #for i in range(numberofpartitions+1) debug here
        while (index1 < numberofpartitions):
            final_tab_n = table_name + str(index1)
            cur.execute("Drop Table If Exists {}".format(final_tab_n))
            cur.execute("Create Table {} (UserID INT, MovieID INT, Rating FLOAT)".format(final_tab_n))
            cur.execute("Insert into {} select userid, movieid, rating from rrobin_temp where index = {}"
                        .format(table_name + str(index1), str(index1)))
            index1 = index1 + 1

        cur.execute("Create Table IF Not Exists rrobin_insert(partition INT, index INT)")
        cur.execute("Insert Into rrobin_insert SELECT {} AS partition, count(*) % {} from {}".format(
             numberofpartitions, numberofpartitions, ratingstablename,))"""

        cur = openconnection.cursor()
        table_name = "rrobin_part"
        #cur = openconnection.cursor()
        global total_part
        total_part = numberofpartitions
        index = 0
        while index < numberofpartitions:
            f_table_name = table_name + str(index)
            cur.execute("Create Table IF Not Exists {} (UserID INT, MovieID INT, Rating REAL)".format(f_table_name))
            index = index + 1

        i = 0
        cur.execute("Select * From {}".format(ratingstablename))
        data = cur.fetchall()
        for row1, row2 in enumerate(data):
            ff_table_name = table_name + str(i)
            cur.execute("Insert Into {}(UserID, MovieID, Rating) Values({}, {}, {})".format(ff_table_name, row2[0], row2[1], row2[2]))
            i = (i + 1) % numberofpartitions

        global last_index
        last_index = i
        openconnection.commit()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    table_name = 'rrobin_part'
    global total_part
    global last_index
    f_table_name = table_name + str(last_index)
    cur.execute("Insert into {} values ({},{},{})".format(f_table_name, userid, itemid, rating))
    cur.execute("Insert into {} values ({},{},{})".format(ratingstablename, userid, itemid, rating))
    last_index = (last_index + 1) % total_part
    openconnection.commit()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    cur.execute("Select MIN(r.part_num) FROM range_insert_table as r where r.starting_rate <= {} and r.ending_rate >= {} "
                .format(rating, rating ))
    index = cur.fetchone()
    starting_partition = index[0]
    cur.execute("Insert into {} values ({},{},{})".format(ratingstablename, userid, itemid, rating))
    cur.execute("Insert into range_part{} values ({},{},{})".format(starting_partition, userid, itemid, rating))
    openconnection.commit()

def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    finally:
        if cursor:
            cursor.close()
