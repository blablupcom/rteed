
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
from lxml import etree

def connect(url):
    report_tree = ''
    try:
        report_html = urllib2.urlopen(url, timeout = 90).read()
        report_tree = etree.HTML(report_html)
    except:
        return connect(url)
    if not report_tree:
        return connect(url)
    else:
        return report_tree



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
    if stop !=0:
        with open(filename, "rb") as fh:
            csv_reader = csv.reader(fh, delimiter=',')
            next(csv_reader)
            next(csv_reader)
            next(csv_reader)
            next(csv_reader)
            next(csv_reader)
            print start, stop, lines
            results = {}
            for row in list(csv_reader)[start:stop]:
                location_url = row[12].replace('https://admin.cqc.org.uk', 'http://www.cqc.org.uk').strip()
                name = row[0].strip()
                add1 = ' '.join(row[2].split(',')[:-1])
                add2 = row[2].split(',')[-1]
                add3 = row[10]
                add4 = row[11]
                postal_code = row[3]
                telephone = row[4]
                type_of_service = row[6]
                services = row[8]
                local_authority = row[11]
                cqc_id = row[14]

                report_soup = connect(location_url)
                latest_report_url = location_url+'/reports'
                latest_report_soup = connect(latest_report_url)

                latest_report = ''
                try:
                    latest_report = latest_report_soup.xpath('//span[@class="visit-date"]/text()')[0].strip() + ' '+ latest_report_soup.xpath('//div[@class=""]//p/text()')[0].strip()
                except:
                    pass
                reports_url = ''
                try:
                    reports_url = report_soup.xpath('//div[@class="overview-inner latest-report"]//li/following-sibling::li//a/@href')[0]
                except:
                    pass
                if 'pdf' not in reports_url:
                    reports_url = ''
                    try:
                        if 'http' not in report_soup.xpath('//a[text()="Read CQC inspection report online"]/@href')[0]:
                            reports_url = 'http://www.cqc.org.uk'+report_soup.xpath('//a[text()="Read CQC inspection report online"]/@href')[0]
                        else:
                            reports_url = report_soup.xpath('//a[text()="Read CQC inspection report online"]/@href')[0]
                    except:
                        pass
                report_date = ''
                try:
                    report_date = report_soup.xpath('//div[@class="overview-inner latest-report"]/h3/text()')[0]
                except:
                    pass
                overview = ''
                try:
                    overview = report_soup.xpath('//div[@class="overview-inspections"]//h3/strong/text()')[0]
                except:
                    try:
                        overview = report_soup.xpath('//div[@class="header-wrapper"]//h2/text()')[0]
                    except:
                        pass
                overview_description = ''
                try:
                    overview_description = report_soup.xpath('//h3[@class="accordion-title"]/following-sibling::div//text()')[0]
                except:
                    pass
                overview_safe = ''
                try:
                    overview_safe = report_soup.xpath('//ul[@class="inspection-results new-inspection"]//a[text()="Safe"]/following-sibling::span/text()')[0]
                except:
                    pass
                overview_effective = ''
                try:
                    overview_effective = report_soup.xpath('//ul[@class="inspection-results new-inspection"]//a[text()="Effective"]/following-sibling::span/text()')[0]
                except:
                    pass
                overview_caring = ''
                try:
                     overview_caring = report_soup.xpath('//ul[@class="inspection-results new-inspection"]//a[text()="Caring"]/following-sibling::span/text()')[0]
                except:
                    pass
                overview_responsive = ''
                try:
                    overview_responsive = report_soup.xpath('//ul[@class="inspection-results new-inspection"]//a[text()="Responsive"]/following-sibling::span/text()')[0]
                except:
                    pass
                overview_well_led = ''
                try:
                    overview_well_led = report_soup.xpath('//ul[@class="inspection-results new-inspection"]//a[text()="Well-led"]/following-sibling::span/text()')[0]
                except:
                    pass
                run_by = ''
                try:
                    run_by = report_soup.xpath('//h3[text()="Who runs this service"]/following-sibling::p/a/text()')[0]
                except:
                    pass
                run_by_url = ''
                try:
                    if 'http' not in report_soup.xpath('//h3[text()="Who runs this service"]/following-sibling::p/a/@href')[0]:
                        run_by_url = 'http://www.cqc.org.uk'+report_soup.xpath('//h3[text()="Who runs this service"]/following-sibling::p/a/@href')[0]
                    else:
                        run_by_url = report_soup.xpath('//h3[text()="Who runs this service"]/following-sibling::p/a/@href')[0]
                except:
                    pass
                overview_summary_url = ''
                try:
                    overview_summary_url = report_soup.xpath('//a[text()="Read overall summary"]/@href')[0]
                except:
                    pass
                overview_summary = summary_safe = summary_effective = summary_caring = summary_responsive = summary_well_led = summary_treating_people_with_respect = summary_providing_care = summary_caring_for_people_safely = summary_staffing = summary_quality_and_suitability_of_management = ''
                if overview_summary_url:
                    overview_summary_url = location_url+'/inspection-summary'
                    overview_summary_soup = connect(overview_summary_url)
                    overview_summary = overview_summary_soup.xpath('//div[@id="overall"]//text()')
                    try:
                        summary_safe = overview_summary_soup.xpath('//div[@id="safe"]//text()')
                    except:
                        pass
                    try:
                        summary_effective = overview_summary_soup.xpath('//div[@id="effective"]//text()')
                    except:
                        pass
                    try:
                        summary_caring = overview_summary_soup.xpath('//div[@id="caring"]//text()')
                    except:
                        pass
                    try:
                        summary_responsive = overview_summary_soup.xpath('//div[@id="responsive"]//text()')
                    except:
                        pass
                    try:
                        summary_well_led = overview_summary_soup.xpath('//div[@id="wellled"]//text()')
                    except:
                        pass

                else:
                    overview_summary_url = location_url+'/inspection-summary'
                    overview_summary_soup = connect(overview_summary_url)

                    try:
                        summary_treating_people_with_respect = overview_summary_soup.xpath('//div[@id="CH1"]/ol//text()')
                    except:
                        pass

                    try:
                        summary_providing_care = overview_summary_soup.xpath('//div[@id="CH2"]/ol//text()')
                    except:
                        pass

                    try:
                        summary_caring_for_people_safely = overview_summary_soup.xpath('//div[@id="CH3"]/ol//text()')
                    except:
                        pass

                    try:
                        summary_staffing = overview_summary_soup.xpath('//div[@id="CH4"]/ol//text()')
                    except:
                        pass
                    try:
                        summary_quality_and_suitability_of_management = overview_summary_soup.xpath('//div[@id="CH5"]/ol//text()')
                    except:
                        pass

                l = [location_url, name, add1, add2, add3, add4,  postal_code, telephone, cqc_id, type_of_service, services, local_authority, latest_report, reports_url, report_date, overview, overview_description, overview_safe, overview_effective,
                                                                 overview_caring, overview_responsive, overview_well_led, run_by, run_by_url, overview_summary, summary_safe, summary_effective, summary_caring, summary_responsive,
                                                                 summary_well_led, summary_treating_people_with_respect, summary_providing_care, summary_caring_for_people_safely, summary_staffing, summary_quality_and_suitability_of_management]
                print name
                results.setdefault(location_url,[]).append(l)


            return results





if __name__ == "__main__":
    st_time = str(datetime.now())
    directoryUrl = "http://www.cqc.org.uk/content/how-get-and-re-use-cqc-information-and-data#directory"
    soup = connect(directoryUrl)
    csvUrl = soup.xpath('//div[@id="directory"]//a/@href')[0]
    response = urllib.urlretrieve(csvUrl)
    start_time = time.time()
    filesize = os.path.getsize(response[0])
    split_size = 4
    pool = mp.Pool(4)
    cursor = 0
    results = []

    with open(response[0], "rb") as fh:
         lines = len(fh.readlines())
         size = lines/split_size
         for chunk in xrange(split_size):
             end = cursor + size
             proc = pool.apply_async(processfile, args=[response[0], lines, cursor, end])
             results.append(proc)
             cursor = end

    pool.close()
    pool.join()
    for proc in results:
        for key, val in proc.get().iteritems():
           for v in val:
                print v[0]
                scraperwiki.sqlite.save(unique_keys=['location_url'], data={"location_url": v[0], "name": unicode(v[1]), "add1": unicode(v[2]), "add2": unicode(v[3]), "add3": unicode(v[4]), "add4": unicode(v[5]),
                                                                            "postal_code": unicode(v[6]), "telephone": unicode(v[7]), "CQC_ID": v[8], "type_of_service": unicode(v[9]), "services": unicode(v[10]), "local_authority": unicode(v[11]), "latest_report": unicode(v[12]), "reports_url": unicode(v[13]),
                                                             "report_date": unicode(v[14]), "overview": unicode(v[15]), "overview_description": unicode(v[16]), "overview_safe": unicode(v[17]), "overview_effective": unicode(v[18]),
                                                             "overview_caring": unicode(v[19]), "overview_responsive": unicode(v[20]), "overview_well_led": unicode(v[21]), "run_by": unicode(v[22]), "run_by_url": unicode(v[23]),
                                                             "overview_summary": unicode(v[24]), "summary_safe": unicode(v[25]), "summary_effective": unicode(v[26]), "summary_caring": unicode(v[27]), "summary_responsive": unicode(v[28]),
                                                             "summary_well_led": unicode(v[29]), 'summary_treating_people_with_respect': unicode(v[30]), 'summary_providing_care': unicode(v[31]), 'summary_caring_for_people_safely': unicode(v[32]), 'summary_staffing': unicode(v[33]), 'summary_quality_and_suitability_of_management': unicode(v[34])
                                                             })


    end_time = str(datetime.now())
    print start_time
    print end_time
