import re
from urllib import FancyURLopener

from bs4 import BeautifulSoup

PHONE_SITE = 'http://gsd-auth-callinfo.s3-website.us-east-2.amazonaws.com/'


class ValidUAOpener(FancyURLopener):
    version = 'Mozilla/5.0 (Windows; U; Windows NT 5.1; it; rv:1.8.1.11)'


class PhoneNumberEntry:
    def __init__(self, phone_number, report_count, comment):
        self.phone_number = re.sub('[^0-9+._ -]+', '', phone_number)
        self.area_code = re.sub('[^0-9]', '', phone_number)[:3]
        self.report_count = report_count
        self.comment = comment.replace('"', '\\"')

    def __unicode__(self):
        skeleton = u'{{ "area_code": "{}", "phone_number": "{}", "report_count": "{}", "comment": "{}" }}'
        return skeleton.format(self.area_code, self.phone_number, self.report_count, self.comment)

    def __str__(self):
        return unicode(self).encode('utf-8')

    def __repr__(self):
        return unicode(self).encode('utf-8')


class Parser:
    def __init__(self, html):
        self.soup = BeautifulSoup(html, 'html.parser')

    def entry_parse(self, html):
        num_of_reports = html.find(class_='oos_previewSide').getText()
        number = html.find(class_='oos_previewHeader').getText()
        comment = html.find(class_='oos_hidden').previousSibling.strip()
        return PhoneNumberEntry(number, num_of_reports, comment)

    def parse(self):
        latest_entries = self.soup.find('ul', id='previews').find_all('li', class_='oos_listItem')
        print(latest_entries)
        return map(self.entry_parse, latest_entries)


# Main
if __name__ == "__main__":
    tmp = ValidUAOpener().open(PHONE_SITE)
    if tmp.code < 202:
        parser = Parser(ValidUAOpener().open(PHONE_SITE).read())
    else:
        print('Please check the URL')




