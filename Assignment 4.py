#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys


RANGE_RATINGS_METADATA = 'rangeratingsmetadata'
ROUND_ROBIN_RATINGS_METADATA = 'roundrobinratingsmetadata'
RANGE_RATINGS_PART = 'RangeRatingsPart'
ROUND_ROBIN_RATINGS_PART = 'RoundRobinRatingsPart'
RANGE_OUTPUT_FILENAME = 'RangeQueryOut.txt'
POINT_OUTPUT_FILENAME = 'PointQueryOut.txt'




# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    cur = openconnection.cursor()
    final_row = []

    #"""Range part"""
    cur.execute("Select PartitionNum From {} Where MinRating between {} AND {} or MaxRating between {} and {}"
        .format(RANGE_RATINGS_METADATA, ratingMinValue, ratingMaxValue, ratingMinValue, ratingMaxValue))
    rows = cur.fetchall()

    for i in rows:
        partition_name = RANGE_RATINGS_PART + str(i[0])
        cur.execute("Select * From {} Where rating >= {} and rating <= {}".format(partition_name, ratingMinValue,
                                                                                 ratingMaxValue))
        initial_row1 = cur.fetchall()
        for x in initial_row1:
            final_row.append([partition_name] + list(x))

    """ROund robin part"""
    cur.execute("Select PartitionNum from {} ".format(ROUND_ROBIN_RATINGS_METADATA))
    int_value_rr = cur.fetchone()[0]
    round_robin_value = int(int_value_rr)

    for i in range(round_robin_value):
        partition_name = ROUND_ROBIN_RATINGS_PART + str(i)
        cur.execute("Select * From {} where rating >= {} and rating <= {}"
                    .format(partition_name, ratingMinValue, ratingMaxValue))
        initial_row2 = cur.fetchall()
        for x in initial_row2:
            final_row.append([partition_name] + list(x))

    writeToFile(RANGE_OUTPUT_FILENAME, final_row)



def PointQuery(ratingsTableName, ratingValue, openconnection):

    cur = openconnection.cursor()
    final_row = []

     #"""range"""
    cur.execute("Select PartitionNum From {0} where MinRating < {1} and MaxRating >= {1}".format(
            RANGE_RATINGS_METADATA, ratingValue))
    rangeValue = cur.fetchone()[0]
    partition_name = RANGE_RATINGS_PART + str(rangeValue)
    cur.execute("Select * From {} Where rating = {} ".format(partition_name, ratingValue))
    initial_row = cur.fetchall()

    for x in initial_row:
        final_row.append([partition_name] + list(x))

     #"""Round robin"""
    cur.execute("Select PartitionNum From {} ".format(ROUND_ROBIN_RATINGS_METADATA))
    round_robin_value = int(cur.fetchone()[0])

    for i in range(round_robin_value):
        partition_name = ROUND_ROBIN_RATINGS_PART + str(i)
        cur.execute("Select * From {} Where rating = {} ".format(partition_name, ratingValue))
        initial_row = cur.fetchall()
        for x in initial_row:
            final_row.append([partition_name] + list(x))

    writeToFile(POINT_OUTPUT_FILENAME, final_row)


def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()
