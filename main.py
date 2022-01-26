#import curl
import os
from math import floor
import logging
import inspect
import datetime as dt
import json
import io
import csv

loggername='BreedCalculation.main' #+ inspect.getfile(inspect.currentframe())
logger = logging.getLogger(loggername)


"""
os.system("rm csv/*.csv") #remove old data
stations=[]


with open("stationen1.csv") as file:
    for line in file:
        stat=[]
        d=line.split(";")
        #if eval(d[-1].replace("\n",""))>20220000:
        if len(d)==5:
            stat=[d[0],d[2]+" "+d[1]] # Bad X
        else:
            stat=[d[0],d[1]]
        stat[1]=stat[1].replace("("," ").replace(")","").replace("/"," ")
        blacklist=[1572,6243,6244,6245,6247,19378]
        if eval(stat[0]) not in blacklist:
            stations.append(stat)


"""

"""for station in stations:
    id=station[0]
	while len(id)<5:
		id="0"+id
	url="https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/recent/tageswerte_KL_"+id+"_akt.zip"
	os.system("wget -O data.zip '"+url+"'")
	os.system("unzip data.zip")
	os.system("mv *produkt_klima_tag* data.csv")
	os.system("rm *.txt")
	os.system("rm *.html")"""
    
def write_json(outfilename, feed):
    try:
        # write values to file
        outfile = open(outfilename, "w+")
        outfile.write(json.dumps(feed, indent=4, sort_keys=True, default=str))
        outfile.close()
        return True
    except Exception as ex:
        logger.exception("Exception " + str(ex))

    return False

def import_csv(filename="feeds.csv",delimiter=","):
    logger = logging.getLogger(loggername + '.' + inspect.currentframe().f_code.co_name)
    feed=[]
    try:
        logger.debug("opening file: '"+ filename +"'")
        lines = 0
        with open(filename, "r") as file:
            fieldnames=[]
            for index, line in enumerate(file):
                linedata={}
                splitline=line.replace("\r", "").replace("\n", "").split(delimiter) 
                if index == 0: #first line contains: "created_at,entry_id,field1,field2,field3,field4,field5,field6,field7,field8,latitude,longitude,elevation,status"
                    fieldnames = splitline
                elif index != 0: #first line contains: "created_at,entry_id,field1,field2,field3,field4,field5,field6,field7,field8,latitude,longitude,elevation,status"
                    for fieldindex, field in enumerate(splitline):
                        linedata[fieldnames[fieldindex]] =  field# dt.datetime.strptime(splitline[0], "%Y-%m-%dT%H:%M:%S%z")
                        """linedata['entry_id'] = splitline[1]
                        for i in range(1, 9):
                            linedata['field' + str(i)]= splitline[i+1]"""
                    #logger.debug("linedata: "+ str(linedata))
                    feed.append(linedata)
                lines=index
        logger.debug("file: '"+ filename +"' contained "+ str(lines) + " lines")
        #os.system("rm data.csv")
    except Exception as ex:
        logger.exception("Exception: "+ str(ex))
    return feed

def write_csv(filename, data, delimiter=","):
    logger = logging.getLogger(loggername + '.' + inspect.currentframe().f_code.co_name)
    try:
        csv_columns = data[0].keys()
        logger.debug(str(csv_columns))
        # Write to CSV File
        write_header = (not os.path.isfile(filename) or os.stat(filename).st_size == 0) # exists or is empty
        with io.open(filename, 'a', newline='', encoding='utf8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns, extrasaction='ignore', delimiter=delimiter, lineterminator='\n')
            if write_header:
                writer.writeheader()  # file doesn't exist yet, write a header
            #writer.writerows(data)
            for row in data:
                writer.writerow(row)

        return True
    except IOError as ex1:
        logger.exception("IOError: "+ str(ex))
    except Exception as ex:
        logger.exception("Exception: "+ str(ex))
    return False

def cleanfeed(feed):
    logger = logging.getLogger(loggername + '.' + inspect.currentframe().f_code.co_name)
    cleanedfeed=[]
    removed_feed=[]
    try:
        logger.debug("feed[0] :" + str(feed[0]))
        old_created_at=dt.datetime.strptime(feed[0]['created_at'], "%Y-%m-%dT%H:%M:%S%z")
        for index,line in enumerate(feed):
            new_created_at = dt.datetime.strptime(line['created_at'], "%Y-%m-%dT%H:%M:%S%z")
            if new_created_at >= old_created_at:
                cleanedfeed.append(line)
                old_created_at = new_created_at
            else: 
                removed_feed.append(line)
    except Exception as ex:
        logger.exception("Exception: "+ str(ex))
    return cleanedfeed, removed_feed


def maxvalue_day(feed, fieldname, delimiter):
    logger = logging.getLogger(loggername + '.' + inspect.currentframe().f_code.co_name)
    maxlist=[]
    try:
        created_at = dt.datetime.strptime(feed[0]['created_at'], "%Y-%m-%dT%H:%M:%S%z")
        old_created_at = dt.date(int(created_at.strftime("%Y")), int(created_at.strftime("%m")), int(created_at.strftime("%d")))
        data = {}
        datalist = []
        for index,line in enumerate(feed):
            created_at = dt.datetime.strptime(line['created_at'], "%Y-%m-%dT%H:%M:%S%z")
            new_created_at = dt.date(int(created_at.strftime("%Y")), int(created_at.strftime("%m")), int(created_at.strftime("%d")))
            if new_created_at == old_created_at:
                datalist.append(feed[index][fieldname])
            elif new_created_at >= (old_created_at+dt.timedelta(days=1)):
                #feed[index]['created_at']
                tempdate = dt.datetime.strptime(feed[index-1]['created_at'], "%Y-%m-%dT%H:%M:%S%z").replace(hour=0, minute=0, second=0, microsecond=0)
                data['created_at'] = tempdate.strftime("%Y-%m-%dT%H:%M:%S%z")
                #data['created_at'] = old_created_at
                logger.debug(str(datalist))
                data[fieldname] = max(datalist)
                datalist = []
                maxlist.append(data)
                data={}
                datalist.append(feed[index][fieldname])
                if not new_created_at == (old_created_at+dt.timedelta(days=1)):
                    missing_days = new_created_at - old_created_at
                    days = missing_days.total_seconds()/(24*60*60)
                    tempdiff = 0
                    logger.critical('An entry for the dates ' + old_created_at.strftime("%Y-%m-%d") + ' and ' + new_created_at.strftime("%Y-%m-%d") + ' is missing at line : '+ str(index) + " this are " + str(days) + ' days')
                    while days > 1:
                        tempdate = tempdate+dt.timedelta(days=1)
                        data['created_at'] = tempdate.strftime("%Y-%m-%dT%H:%M:%S%z")
                        data[fieldname] = -1
                        maxlist.append(data)
                        logger.critical('Fake entry for the dates ' + data['created_at'] + " added")
                        
                        data={}
                        days = days - 1
                old_created_at = new_created_at
            else:
                logger.critical('An entry in irregular order found at the dates ' + old_created_at.strftime("%Y-%m-%d") + ' and ' + new_created_at.strftime("%Y-%m-%d") + ' is missing at line : '+ str(index))
                old_created_at = new_created_at
    except Exception as ex:
        logger.exception("Exception: "+ str(ex))
    return maxlist

def check_maxlist(feed, fieldname):
    logger = logging.getLogger(loggername + '.' + inspect.currentframe().f_code.co_name)
    maxlist=[]
    try:
        print("Nothing")
    except Exception as ex:
        logger.exception("Exception: "+ str(ex))
    return maxlist


def process_values(maxlist, fieldname, strstartdate):
    logger = logging.getLogger(loggername + '.' + inspect.currentframe().f_code.co_name)
    try:

        #data=[]
        temp=[]
        date=[]
        startdate = dt.datetime.strptime(strstartdate+"T00:00:00+01:00", "%Y-%m-%dT%H:%M:%S%z")
        for day in maxlist:
            datetime = dt.datetime.strptime(day['created_at'], "%Y-%m-%dT%H:%M:%S%z")
            if day!=maxlist[0]:
                if datetime > startdate:
                    temp.append(float(day[fieldname]))
                    date.append(datetime)


        new_egg=[]
        eggs=[0]
        brood=[0]
        new_bees=[0]
        bees=[8000]
        for t in temp:
            if t>0:
                new_egg.append(floor(22.8295*t**1.4254))
            else:
                new_egg.append(0)
            if len(new_egg)<10:
                eggs.append(sum(new_egg))
                brood.append(brood[-1])
                new_bees.append(0)
            if len(new_egg)>9 and len(new_egg)<21:
                eggs.append(sum(new_egg[-9:]))
                brood.append(sum(new_egg))
                new_bees.append(0)
            if len(new_egg)>20:
                eggs.append(sum(new_egg[-9:]))
                brood.append(sum(new_egg[-21:]))
                new_bees.append(new_egg[-21])
        bees=[]
        for i in range(len(new_bees)-1):
            if bees==[]:
                bees=[8000]
            else:
                month=int(date[i].strftime("%m"))
                rate=0
                if month<3 or month>10:
                    rate=1./50
                if month>2 and month<11:
                    rate=1./100
                logger.debug("rate: " + str(rate) + " bees yesterday: " + str(bees[-1]) + " startbees today: " + str(bees[-1]*(1-rate)))
                temp_bees=bees[-1]*(1-rate)+new_bees[i] 
                bees.append(int(temp_bees))
        #os.system("rm csv/*.csv")
        #	os.system("touch 'csv/"+station[1].replace(" ","")+".csv'")
        with open("csv/"+"out.csv" , "w") as file:
            #file.write("#\n#based on data by DWD - Deutscher Wetterdienst under their Opendata regulation\n#https://opendata.dwd.de/\n 
            file.write('"Datum";"Tagesmaximaltemperatur";"Stifte";"Brut gesammt";"neue Bienen";"Bienen gesamt"\n')
            for i in range(len(eggs)-1):
                file.write(date[i].strftime("%Y-%m-%d")+";"+str(temp[i]).replace(".", ",")+";"+str(eggs[i])+";"+str(brood[i])+";"+str(new_bees[i])+";" + str(bees[i]) + "\n" )
    except Exception as ex:
        logger.exception("Unhandled Exception: " + str(ex))


def main():
    logger = logging.getLogger(loggername + '.' + __name__)
    try:
        folder="csv/"
        filename="feeds.csv"
        delimiter=";"
        fieldname = 'field6'
        outfilename = 'raw'
        strstartdate = '2021-07-21'
        logging.basicConfig(level=logging.DEBUG)
        feed = import_csv(filename, delimiter)
        if len(feed) > 0: 
            feed, removed_feed = cleanfeed(feed)
            if len(removed_feed) > 0:
                write_json(folder+'removed_'+outfilename+'.json', removed_feed)
                write_csv(folder+'removed_'+outfilename+'.csv', removed_feed, delimiter)
            if len(feed) > 0:
                write_json(folder+outfilename+'.json', feed)
                write_csv(folder+outfilename+'.csv', feed)
            
            maxlist = maxvalue_day(feed, fieldname, delimiter)
            delimiter=";"
            if len(maxlist) > 0:
                write_json(folder+'maxlist_'+outfilename+'.json', maxlist)
                write_csv(folder+'maxlist_'+outfilename+'.csv', maxlist, delimiter)
                process_values(maxlist, fieldname, strstartdate)

    except Exception as ex:
        logger.exception("Unhandled Exception: " + str(ex))

if __name__ == '__main__':
    try:
        main()

    except (KeyboardInterrupt, SystemExit):
        logger.debug("exit")
        exit
    except Exception as ex:
        logger.error("Unhandled Exception in "+ __name__ + repr(ex))