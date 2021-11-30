#
# Assignment2 Interface
#

import psycopg2
import os
import sys
import concurrent.futures

# Function used by the thread
def thread_function(rect_point_tuple):
    rectangle = rect_point_tuple[0]
    point = rect_point_tuple[1]
    join_fragment_number = rect_point_tuple[2]
    connection = rect_point_tuple[3]
    cur = connection.cursor()
    join_table_name = "JOIN" + str(join_fragment_number)
    group_table_name = "GROUP" + str(join_fragment_number)

    # Joining results between fragments of Rectangles and Points
    cur.execute("DROP TABLE IF EXISTS " + join_table_name)
    points_select_query = point + ".longitude, " + point + ".latitude, " + point + ".geom as geom2"
    rectangle_select_query = rectangle + ".longitude1, " + rectangle + ".latitude1, " + \
                             rectangle + ".longitude2, " + rectangle + ".latitude2," + \
                             rectangle + ".geom as geom1"
    query = "CREATE TABLE " + join_table_name + " AS (" + \
            "SELECT " + rectangle_select_query + ", " + points_select_query + " FROM " + \
            rectangle + "," + point + " WHERE ST_Contains(" + rectangle + ".geom" + ", " + point + ".geom" + "))"
    cur.execute(query)
    connection.commit()

    # Grouping results on a thread (between fragment on Points and Rectangle)
    cur.execute("DROP TABLE IF EXISTS " + group_table_name)
    query = "CREATE TABLE " + group_table_name + " AS (SELECT COUNT(*) as COUNTER, geom1 FROM " + \
            join_table_name + " GROUP BY geom1 ORDER by counter ASC)"
    cur.execute(query)
    connection.commit()


# Do not close the connection inside this file i.e. do not perform openConnection.close()
def parallelJoin(pointsTable, rectsTable, outputTable, outputPath, openConnection):
    # Creating variables for min, max of longitudes on points, rectangles
    min_points_longitude, max_points_longitude = None, None
    min_rectangles_longitude1, max_rectangles_longitude1, min_rectangles_longitude2, max_rectangles_longitude2 = \
        None, None, None, None

    # Creating a cursor on the connection
    cur = openConnection.cursor()

    # Fetching min,max of longitude, longitude2 from rectangles
    cur.execute("SELECT MIN(longitude1), MAX(longitude1), MIN(longitude2), MAX(longitude2)  FROM " + rectsTable)
    min_rectangles_longitude1, max_rectangles_longitude1, min_rectangles_longitude2, max_rectangles_longitude2 = \
        cur.fetchone()
    min_rectangles_longitude_overall = min([min_rectangles_longitude1, max_rectangles_longitude1,
                                            min_rectangles_longitude2, max_rectangles_longitude2])

    max_rectangles_longitude_overall = max([min_rectangles_longitude1, max_rectangles_longitude1,
                                            min_rectangles_longitude2, max_rectangles_longitude2])

    # Obtaining partition interval size
    partition_size = (abs(abs(max_rectangles_longitude_overall) - abs(min_rectangles_longitude_overall))) / 4.0
    partition1_range = (min_rectangles_longitude_overall, min_rectangles_longitude_overall + partition_size)
    partition2_range = (partition1_range[1], partition1_range[1] + partition_size)
    partition3_range = (partition2_range[1], partition2_range[1] + partition_size)
    partition4_range = (partition3_range[1], max_rectangles_longitude_overall)
    partition_ranges = [partition1_range, partition2_range, partition3_range, partition4_range]

    # Creating fragments on both rectangles and points table
    for index in range(0, len(partition_ranges)):
        partition_range = partition_ranges[index]

        rects_partition_table_name = "R" + str(index)
        cur.execute("DROP TABLE IF EXISTS " + rects_partition_table_name)
        query = "CREATE TABLE " + rects_partition_table_name + " AS (" + \
                "SELECT * " + " FROM " + rectsTable + " WHERE " + "((longitude1 >= " + \
                str(partition_range[0]) + " AND longitude1 <= " + str(partition_range[1]) + ") OR (longitude2 >= " + \
                str(partition_range[0]) + " AND longitude2 <= " + str(partition_range[1]) + ")))"
        cur.execute(query)
        openConnection.commit()

        points_partition_table_name = "P" + str(index)
        cur.execute("DROP TABLE IF EXISTS " + points_partition_table_name)
        query = "CREATE TABLE " + points_partition_table_name + " AS (" + \
                "SELECT * FROM " + pointsTable + " WHERE " + "longitude >= " + \
                str(partition_range[0]) + " AND longitude <= " + str(partition_range[1]) + ")"
        cur.execute(query)
        openConnection.commit()

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        executor.map(thread_function, [("R0", "P0", "0", openConnection)])
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        executor.map(thread_function, [("R1", "P1", "1", openConnection)])
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        executor.map(thread_function, [("R2", "P2", "2", openConnection)])
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        executor.map(thread_function, [("R3", "P3", "3", openConnection)])

    # Union results of 4 parallel joins
    cur.execute("DROP TABLE IF EXISTS UNION_TABLE")
    query = "CREATE TABLE UNION_TABLE AS (SELECT * FROM group0 UNION " \
            "SELECT * FROM group1 UNION " \
            "SELECT * FROM group2 UNION " \
            "SELECT * FROM group3)"
    cur.execute(query)
    openConnection.commit()

    # Group by to find the final counts, saving results in DB
    cur.execute("DROP TABLE IF EXISTS " + outputTable)
    query = "CREATE TABLE " + outputTable + " AS (" + \
            "SELECT sum(counter) as points_count, geom1 FROM UNION_TABLE group by geom1 ORDER BY points_count ASC)"
    cur.execute(query)
    openConnection.commit()

    # Fetching results and saving them in file
    query = "SELECT points_count from " + outputTable
    cur.execute(query)
    final_results = cur.fetchall()

    with open(outputPath, 'w') as f:
        f.writelines([str(item[0]) + "\n" for item in final_results])


################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='12345', dbname='dds_assignment2'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


# Donot change this function
def createDB(dbname='dds_assignment2'):
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
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.commit()
    con.close()


# Donot change this function
def deleteTables(tablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if tablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (tablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()
