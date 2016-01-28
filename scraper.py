# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import scraperwiki
import urllib2
from datetime import datetime
from bs4 import BeautifulSoup
import csv
import re
import os, multiprocessing as mp
import urllib
import time


def connect(url):
    report_soup = ''
    try:
        report_html = urllib2.urlopen(url)
        report_soup = BeautifulSoup(report_html, 'lxml')
    except:
        connect(url)
    if not report_soup:
        connect(url)
    else:
        return report_soup



def processfile(filename, lines, start=0, stop=0):
    if start == 0 and stop == 0:
        with open(filename, "r") as fh:
            csv_reader = csv.reader(fh, delimiter=',')
            selection = [row for row in reader]
            lines = fh.readlines(stop - start)
            # print(lines)
            for row in lines:
                print row.split(',')[0]
                if 'http://' not in row[12]:
                    continue

                location_url = row[12].replace('https://admin.cqc.org.uk', 'http://www.cqc.org.uk')
                name = row[0]
                add1 = ' '.join(row[2].split(',')[:-1])
                add2 = row[2].split(',')[-1]
                add3 = row[10]
                add4 = row[11]
                postal_code = row[3]
                print name
    else:
        with open(filename, "r") as fh:
            csv_reader = csv.reader(fh, delimiter=',')
            print start, stop, lines
            results = {}

            # return list(csv_reader)[start:stop]
            for row in list(csv_reader)[start:stop]:
                # print row
                if 'http' not in row[12]:
                    continue
                location_url = row[12].replace('https://admin.cqc.org.uk', 'http://www.cqc.org.uk')
                name = row[0]
                # add3 = row[10]
                report_soup = connect(location_url)
                report_date = ''
                try:
                    report_date = report_soup.find('div', 'overview-inner latest-report').find('h3').text.strip()
                except:
                    pass
                print name
                results[name]= report_date
            return results
                # report_soup = connect(location_url)
                # latest_report_url = location_url+'/reports'
                # latest_report_soup = connect(latest_report_url)
                # latest_report = ''
                # try:
                #     latest_report = latest_report_soup.find('h2', text=re.compile('Reports')).find_next('div').text.strip()
                # except:
                #     pass
                # reports_url = ''
                # try:
                #     reports_url = report_soup.find('div', 'overview-inner latest-report').find('li').find_next('li').find('a')['href']
                # except:
                #     pass
                # if 'pdf' not in reports_url:
                #     reports_url = ''
                #     try:
                #         reports_url = 'http://www.cqc.org.uk'+report_soup.find('a', text=re.compile('Read CQC inspection report online'))['href']
                #     except:
                #         pass
                # report_date = ''
                # try:
                #     report_date = report_soup.find('div', 'overview-inner latest-report').find('h3').text.strip()
                # except:
                #     pass
                # print name
                # scraperwiki.sqlite.save(unique_keys=['name'], data={"name": unicode(name)})
            # #      results.append(row)
            #     if i == lines:
            #         break
            # print results
            # return results

            # selection = [row for row in reader[start:stop]]
            # print selection
            # lines = fh.readlines(stop - start)
            # print lines
            # p = 0
            # for row in lines:
            #     print row.split(',')[0]
                # if 'http://' not in row.split(',')[12]:
                #     continue
                # location_url = row[12].replace('https://admin.cqc.org.uk', 'http://www.cqc.org.uk')
                # name = row.split(',')[0]
                # add1 = ' '.join(row[2].split(',')[:-1])
                # add2 = row[2].split(',')[-1]
                # add3 = row[10]
                # add4 = row[11]
                # postal_code = row[3]
                # print name
                # p+=1

                # scraperwiki.sqlite.save(unique_keys=['name'], data={"name": unicode(name)})




if __name__ == "__main__":
    st_time = time.time()
    directoryUrl = "http://www.cqc.org.uk/content/how-get-and-re-use-cqc-information-and-data#directory"
    soup = connect(directoryUrl)
    block = soup.find('div',{'id':'directory'})
    csvA = block.find('a',href=True)
    csvUrl = csvA['href']
    print csvUrl
    response = urllib.urlretrieve(csvUrl)
    start_time = time.time()
    filesize = os.path.getsize(response[0])
    # print filesize
    split_size = 4

    # result = processfile(response[0])
    # print result
    # print filesize, split_size
    if filesize > split_size:
        pool = mp.Pool(4)
        cursor = 0
        results = []

        with open(response[0], "r") as fh:
             lines = len(fh.readlines())
             size = lines/split_size
             print size
        #      spamreader = csv.reader(fh, delimiter=',')
             for chunk in xrange(split_size):

                 # if cursor + split_size > filesize:
                 #     end = filesize
                 # else:
                 end = cursor + size
        #
        #          fh.seek(end)
        #          fh.readline()
        #          end = fh.tell()

                 proc = pool.apply_async(processfile, args=[response[0], lines, cursor, end])
                 results.append(proc)
                 # for p in proc.get():
                 #     print(p)
                 cursor = end
                 # print cursor
        pool.close()
        pool.join()
        # p=0

        for proc in results:
            for key, val in proc.get().iteritems():
               # print name
               todays_date = str(datetime.now())
            #   write.writerow([key, val])
               
               scraperwiki.sqlite.save(unique_keys=['d'], data={"d": todays_date, "name": unicode(key), "val": unicode(val)})
        end_time = st_time-time.time()
