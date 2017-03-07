#imports
from __future__ import print_function
#sql imports
import pyodbc

#system imports
import argparse
import sys
import random
import os

#time imports
import datetime
import timeit

#color class for handling text colors
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
#class for terminal labels
class labels:
    OKBLUE = "[ " + bcolors.OKBLUE + "OK" + bcolors.ENDC + " ] "
    OKGREEN = "[ " + bcolors.OKGREEN + "OK" + bcolors.ENDC + " ] "
    WARNING = "[ " + bcolors.WARNING + "WARN" + bcolors.ENDC + " ] "
    FAIL = "[ " + bcolors.FAIL + "FAIL" + bcolors.ENDC + " ] "
    DEBUG = "[ " + bcolors.BOLD + "FAIL" + bcolors.ENDC + " ] "
#class for storing result data
class Result:
    def __init__(self,resultArray,name):
        self.resultList = resultArray
        self.name = name

    def getAvg(self):
        total = self.getTotal()
        avg = total / len(self.resultList)
        return avg

    def getTotal(self):
        return sum(self.resultList)

    def getMax(self):
        return max(self.resultList)

    def getMin(self):
        return min(self.resultList)

    def getEntries(self):
        return len(self.resultList)

#setting all arguments
parser = argparse.ArgumentParser(prog="SQL Stresstest")

parser.add_argument('-db', '--database', type=str, help="database name")
parser.add_argument('-usr', '--user', type=str, help="database user")
parser.add_argument('-p', '--port', type=str, help="database port")
parser.add_argument('-pass', '--password', type=str, help="database password")
parser.add_argument('-drv', '--driver', type=str, help="database driver")
parser.add_argument('-ip', '--ip', type=str, help="database server ip")
parser.add_argument('-tba', '--tableAmount', type=str, help="amount of tables created during the test")
parser.add_argument('-ts', '--tableSize', type=str, help="amount of entries get added into each table")
parser.add_argument('-c', '--concurrency', type=int, help="amount of concurrency during qurey tests")
parser.add_argument('-i', '--iterations', type=int, help="amount of iterations the query tests will run")
parser.add_argument('-out', '--outputfile', type=str, help="file the results are written to")
parser.add_argument('-autoc', '--autocommit', type=bool, help="autocommit transactions true/false")

#parsing the inserted arguments
args = parser.parse_args();

#global variables
tables = []
tableAmountDefault = 3
databaseDefault = "TestDatabase"
driverDefault = '{FreeTDS}'
tableSizeDefault = 50000
conn = ""
cur = ""
db = ""
outputfileDefault = "StresstestResults.txt"
connected = False
iterations = 10000
concurrency = 50
loopIndex = 0
results = []

#namelists
firstnameList = [
'kees',
'henk',
'piet',
'jan',
'willem',
'tom'
]
#list of random lastnamesS
lastnameList = [
'van_dam',
'de_fries',
'pietersburg',
'van_der_velde'
]
#list of random user types
typeList = [
    'customer',
    'company',
    'organization',
    'charity',
    'foundation'
]
#list of random lists...
ListList = [
 "firstname",
 "lastname",
 "type",
]

###-----------------------------------------------------------------------------

#checks if all needed arguments are inserted
def checkArgs():
    #checks the neccecary components
    if None in [args.ip, args.password, args.user]:
        print(labels.FAIL + "arguments missing, please fill in all arguments")
        closeProgram()
    else:
        #sets defaults
        if args.driver == None:
            args.driver = driverDefault
            print(labels.WARNING + "no driver given using default : "+ driverDefault)
        if args.database == None:
            print(labels.WARNING + "no database given using default: " + databaseDefault)
            args.database = databaseDefault
        if args.tableAmount == None:
            args.tableAmount = tableAmountDefault
            print(labels.WARNING + "no table amount specified, using default: " + str(tableAmountDefault) )
        if args.autocommit == None:
            args.autocommit = False
            print(labels.WARNING + "autocommit=False")
        else:
            try:
                args.tableAmount = int(args.tableAmount)
            except ValueError:
                print (labels.FAIL + "invalid table amount specified, using default : " + str(tableAmountDefault) )
                args.tableAmount = tableAmountDefault
        if args.tableSize == None:
            args.tableSize = tableSizeDefault
            print(labels.WARNING + "no table size given, using default: " + str(tableSizeDefault))
        else:
            try:
                args.tableSize = int(args.tableSize)
            except ValueError:
                print (labels.FAIL + "invalid table amount specified, using default : " + str(tableSizeDefault) )
                args.tableSize = tableSizeDefault
        if args.iterations == None:
            args.iterations = iterations
            print(labels.WARNING + "no iteration amount specified, using default : " + str(iterations))
        else:
            try:
                args.iterations = int(args.iterations)
            except ValueError:
                print(labels.FAIL + "invalid iteration amount specified using default : " + str(iterations))
                args.iterations = iterations
        if args.port == None:
            args.port = ""
            print(labels.WARNING + "no port specified, connecting without port")
        if args.concurrency == None:
            args.concurrency = concurrency
            print(labels.WARNING + "no concurrency amount specified, using default : " + str(concurrency))
        else:
            try:
                args.concurrency = int(args.concurrency)
            except ValueError:
                print(labels.FAIL + "invalid concurrency amount specified using default : " + str(concurrency))
                args.concurrency = concurrency
        if args.outputfile == None:
            args.outputfile = outputfileDefault
            print(labels.WARNING + "no outpur file specified, using default: " + outputfileDefault)

#connects to the database server
def connectToServer():
    print(labels.OKBLUE + "Connecting to server at: " + args.ip + " at port: " + args.port)
    global conn
    global connected
    connString = ""
    connString = connString + 'Driver=' + args.driver
    connString = connString +';Server=' + args.ip
    if not args.port == "":
        connString = connString + ';Port=' + args.port
    connString = connString + ';Uid=' + args.user
    connString = connString + ';Pwd=' + args.password
    connString = connString + ';Encrypt=yes;Connection Timeout=30'
    if args.autocommit:
        connString = connString + ";autocommit=True"
    try:
        #print(connString) #for debugging
        conn = pyodbc.connect(connString)
    except pyodbc.Error as ex:
        for arg in ex.args:
            print(labels.FAIL + arg )
        sys.exit()
    print(labels.OKGREEN + "Connection OK" )
    connected = True
    global cur
    cur = conn.cursor() #creates a cursor object to preform queries from

#connects to the database
def connectToDB():
    print(labels.OKBLUE + "Connecting to " + args.database + " at: " + args.ip + " at port: " + args.port)
    global conn
    global connected
    connString = ""
    connString = connString + 'Driver=' + args.driver
    connString = connString +';Server=' + args.ip
    if not args.port == "":
        connString = connString + ';Port=' + args.port
    connString = connString + ';Uid=' + args.user
    connString = connString + ';Pwd=' + args.password
    connString = connString + ';Encrypt=yes;Connection Timeout=30'
    connString = connString + ';DATABASE=' + args.database
    if args.autocommit:
        connString = connString + ";autocommit=True"
    try:
        #print(connString) #for debugging
        conn = pyodbc.connect(connString)
    except pyodbc.Error as ex:
        for arg in ex.args:
            print(labels.FAIL + arg )
        sys.exit()
    print(labels.OKGREEN + "Connection OK" )
    connected = True
    global cur
    cur = conn.cursor() #creates a cursor object to preform queries from

#creates the tables
def createTestTables():
    print(labels.OKBLUE + "generating test tables" )
    for x in range(0, int(args.tableAmount)):
        #unique table names are generated from timestamp + loop index
        st = datetime.datetime.now()
        tablename = "testTable" + str(st) + str(x)
        tablename = tablename.replace(" ","")
        tablename = tablename.replace("-","")
        tablename = tablename.replace(":","")
        tablename = tablename.replace(".","")
        tables.append(tablename)
        print(labels.OKGREEN + "created table : " + tablename )
    print(labels.OKGREEN + "done generating tables" )
    #insertTestTables() #function which should be called next

#inserts the tables into the database
def insertTestTables():
    print(labels.OKBLUE + "inserting tables to Database" )
    for table in tables:
        insertString = "CREATE TABLE " + table + "(firstname varchar(255), lastname varchar(255), type varchar(255), randomValue varchar(255), customerId varchar(255))"
        executeCursor(insertString, "inserting tables into database")
    print(labels.OKGREEN + "Succesfully inserted tables into Database")

#creates the given database on the server
def createDatabase():
    print(labels.OKBLUE + "creating new database: " + args.database)
    insertString = "CREATE DATABASE " + args.database
    executeCursor(insertString, "creating new database")
    print(labels.OKGREEN + "Succesfully created new database")

#fills the tables with random entries
def fillTables():
    global conn
    global loopIndex
    print(labels.OKBLUE + "filling tables...")
    startTime = datetime.datetime.now()
    print(labels.OKGREEN + "start time: " + str(startTime))
    printProgressBar(0, args.tableSize, prefix = 'Progress:', suffix = 'Complete', length = 50)
    #notes the start time
    totalDur = 0
    avgs = []
    for table in tables:
        loopIndex = 0
        tim = []
        for x in range(args.tableSize):
            tim.extend(addEntryToTable(table))

        tableResult = Result(tim,"fill-" + table)
        results.append(tableResult)
        print(labels.OKGREEN + "filled table: " + table + " in " + str(tableResult.getTotal()) + " seconds, " + str(tableResult.getAvg()) + " seconds per entry, " + str(args.tableSize) + " entries")
        totalDur = totalDur + tableResult.getTotal()
        avgs.append(tableResult.getAvg())
    #notes the end time
    endTime = datetime.datetime.now()
    print(labels.OKGREEN + "end time: " + str(endTime))
    #calculates the duration
    duration = endTime - startTime
    tot = sum(avgs)
    avg = tot / len(avgs)
    conn.commit()
    print(labels.OKGREEN + "duration: " + str(duration) + " [ total: " + str(totalDur) + " sec - avg-per-entry: " + str(avg) + " sec ] ")

#preforms a series of query tests
def queryStressTest():
    global loopIndex
    ###light qeury test
    print(labels.OKBLUE + "starting light query test")
    printProgressBar(0, args.iterations * args.concurrency, prefix = 'Progress:', suffix = 'Complete', length = 50)
    startTime = datetime.datetime.now()
    loopIndex = 0
    tim = []
    for x in range (args.iterations):
        tim.extend(lightQueryTest())
    testResult = Result(tim,"Light Query test")
    results.append(testResult)
    endTime = datetime.datetime.now()
    duration = endTime - startTime
    print(labels.OKGREEN + " completed light query test, duration : " + str(duration) + " [ total: " + str(testResult.getTotal()) + " sec - avg: " + str(testResult.getAvg()) + " sec, " + str(testResult.getEntries()) +" queries ]")
    ###medium query test

    ###heavy query test

#preforms a lightquery
def lightQueryTest():
    rdtable = random.choice(tables)
    rdlist = random.choice(ListList)
    if rdlist == 'firstname':
        rdvalue = random.choice(firstnameList)
    elif rdlist == 'lastname':
        rdvalue = random.choice(lastnameList)
    elif rdlist == "type":
        rdvalue = random.choice(typeList)
    else:
        rdvalue = " "

    queryString = "SELECT * FROM " + rdtable + " WHERE " + rdlist + "='" + rdvalue + "'"
    T = timeit.Timer('executeCursor("' + queryString + '","Selecting data from database for light query test")',"from __main__ import executeCursor")
    t = T.repeat(1,args.concurrency)
    printProgressBar(loopIndex, args.iterations * args.concurrency, prefix = 'Progress:', suffix = 'Complete', length = 50)
    return t

#preforms a light update to the database
def lightUpdateTest():
    global loopIndex
    rdtable = random.choice(tables)
    rdlist = random.choice(ListList)
    if rdlist == 'firstname':
        rdvalue = random.choice(firstnameList)
        rdvalue2 = random.choice(firstnameList)
    elif rdlist == 'lastname':
        rdvalue = random.choice(lastnameList)
        rdvalue2 = random.choice(lastnameList)
    elif rdlist == "type":
        rdvalue = random.choice(typeList)
        rdvalue2 = random.choice(typeList)
    else:
        rdvalue = " "
        rdvalue2 = " "

    queryString = "UPDATE " + rdtable + " SET " + rdlist + "='" + rdvalue2 + "' WHERE " + rdlist + "='" + rdvalue + "'"
    T = timeit.Timer('executeCursor("' + queryString + '","Update data in database for light update test")',"from __main__ import executeCursor")
    t = T.repeat(1,args.concurrency)
    printProgressBar(loopIndex, args.iterations * args.concurrency, prefix = 'Progress:', suffix = 'Complete', length = 50)
    return t
#adds an random entry to table
def addEntryToTable(table):
    global loopIndex
    err = "attemping to fill tables with entries"
    insertString = "INSERT INTO " + table + " (firstname, lastname, type, randomValue, customerId) VALUES ('" + random.choice(firstnameList) + "','" + random.choice(lastnameList) + "','" + random.choice(typeList) + "'," + str(random.randint(0,args.iterations)) + "," + str(loopIndex) + ")"
    T = timeit.Timer('executeCursor("' + insertString + '","attemping to fill tables with entries")',"from __main__ import executeCursor")
    time = T.repeat(1,1)
    printProgressBar(loopIndex, args.tableSize, prefix = 'Progress:', suffix = 'Complete', length = 50)
    return time

#preforms an sequence of updates
def updateStressTest():
    global loopIndex
    global conn
    print(labels.OKBLUE + "starting updateTest...")
    loopIndex = 0;
    printProgressBar(0, args.iterations, prefix = 'Progress:', suffix = 'Complete', length = 50)
    tim = []
    for x in range(args.iterations):
        tim.extend(lightUpdateTest())
    testResult = Result(tim,"light update test")
    results.append(testResult)
    conn.commit()
    print(labels.OKGREEN + "Completed light update test [ total: " + str(testResult.getTotal()) + " sec - avg: " + str(testResult.getAvg()) + " sec, " + str(testResult.getEntries()) +" updates ]")

#creates a new files and places the results in there
def createResultFile():
    print(labels.OKBLUE + "creating result file...")
    file = open(args.outputfile,'w')
    file.write("results:\n \n")
    for result in results:
        file.write(result.name + " = { \n")
        file.write("total: " + str(result.getTotal()) + ",\n")
        file.write("average: " + str(result.getAvg()) + ",\n")
        file.write("min: " + str(result.getMin()) + ",\n")
        file.write("max: " + str(result.getMax()) + ",\n")
        file.write('queries: ' + str(result.getEntries()) + ",\n")
        file.write("}\n")
        file.write("\n")
    file.close()
    print(labels.OKGREEN + "Succesfully created result file : " + args.outputfile )

###-----------------------------------------------------------------------------

#closes the program and all connections it had running
def closeProgram():
    print(labels.OKBLUE + "closing program.." )
    global connected
    if connected:
        cleanUp()
        closeConnections()
    print(labels.OKGREEN + "Done")
    sys.exit()

#closes the connections
def closeConnections():
    print(labels.OKBLUE + "closing connections")
    cur.close()
    conn.close()

#helper function for inserting data into database
def executeCursor(executeString, err):
    global cur
    global conn
    global loopIndex
    try:
        loopIndex = loopIndex + 1
        cur.execute(executeString)
    except pyodbc.Error as er:
        conn.rollback()
        for e in er:
            print(e)
        print(labels.FAIL + "error occured while " + err + ", closing program.." )
        closeProgram()

#removes everything the program has created
def cleanUp():
    print(labels.OKBLUE + "cleaning up..")
    insertString = "DROP DATABASE " + args.database
    executeCursor(insertString, "removing database")

# Print iterations progress -- code borrowed from http://stackoverflow.com/questions/3173320/text-progress-bar-in-the-console
def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '|'):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end = '\r')
    # Print New Line on Complete
    if iteration == total:
        print()

#prints the results to the console
def printResults():
    print("results: ")
    for result in results:
        print(result.name, "tot: " + str(result.getTotal()), "avg: " + str(result.getAvg()), "max: " + str(result.getMax()), "min: " + str(result.getMin()), "queries: " + str(result.getEntries()) )


#code that actually gets run

###setup connection
sys.setrecursionlimit(50000)
checkArgs() #confirms the input
connectToServer() #creates and confirms a connection to the database server
###setup test environment
createDatabase() #creates a new DB
connectToDB() #connects to the new database
createTestTables() #generates unique table names
insertTestTables() #inserts the previously generated tables into the DB
###actual testing
fillTables() #fills the tables with the specified amount of entries
queryStressTest() #light query test
updateStressTest() #light update test
##wrapping up
createResultFile() #creates a file with the test results
printResults() #prints the results to the console
closeProgram() #cleans up and closes the program
