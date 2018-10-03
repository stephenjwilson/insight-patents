"""

Much of the parsing code was based loosely on https://github.com/iamlemec/patents/
"""
import datetime
import re
from io import BytesIO
from itertools import chain
from IPython import embed
from lxml import etree
from pyspark import SparkContext

from src.python.patent import Patent
#from patent import Patent


class PatentParser(object):
    """
    This class handles the parsing of patents from the USTPO and uses the Patent class to store and
    manipulate the data.
    """

    # TODO: add support for chemicals
    # TODO: Ensure it's a grant?
    def __init__(self, file_name, detect_format=True):
        """
        This parsing class takes a file name and will detect the format of the file based on the extension
        if desired. If it doesn't detect the format, it assumes the latest format.
        Once it has the format, it will parse the patent file into a list of patents of type
        Patent
        :param file_name:
        :param detect_format:
        """
        sc = SparkContext.getOrCreate()
        log4jLogger = sc._jvm.org.apache.log4j
        log = log4jLogger.LogManager.getLogger(__name__)
        # Passed Parameters
        self.file_name = file_name

        # Other Parameters - to be defined
        self.generation = None
        self.parser = self.parse_v3  # default is v3 XML
        self.patents = []
        self.totalpatents = 0
        self.citationpatents = 0
        if detect_format:
            # Check file format / generation
            self.determine_patent_type()

        # Parse the data into a list of patents
        try:
            self.parser()
        except Exception as e:
            log.warn('Failed to parse {} with exception {}'.format(file_name, e))

    @staticmethod
    def get_child_text(element, tag, default=''):
        """
        Get the text of a child node
        :param element: the lxml element object
        :param tag: the tag to find
        :param default: the default string to return to the parser
        :return:
        """
        child = element.find(tag)
        return (child.text or default) if child is not None else default

    @staticmethod
    def raw_text(element, sep=''):
        """
        Get all text of node
        :param par:
        :param sep:
        :return:
        """
        return sep.join(element.itertext()).strip()

    @staticmethod
    def clear(elem):
        """
        Preserves the memory for a process
        :param elem:
        :return:
        """
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]

    @staticmethod
    def acceptable(line):
        """
        Determines if it is a line of xml that is acceptable
        :param line: a line of an XML file
        :return:
        """
        if line.startswith('<?xml'):
            return False
        elif line.startswith('<!DOCTYPE'):
            return False
        elif line.startswith('<!ENTITY') or line.startswith(']>'):
            return False
        else:
            return True

    @staticmethod
    def clean_date(date):
        """
        Cleans dates extracted from patents. Return the NULL 1900-01-01 if it fails to format the date
        :param date:
        :return:
        """
        if len(date) > 8:
            date = '19000101'
        if date[-2:] == '00':  # Days can't be 0
            date = date[:-2] + '01'
        if int(date[:2]) != 20 and int(date[:2]) != 19:
            date = date[1] + date[0] + date[2:]  # Fix an easy transposition
            if int(date[:2]) != 20 and int(date[:2]) != 19:
                date = '19000101'
        try:
            date = datetime.datetime.strptime(str(date), '%Y%m%d').strftime('%Y-%m-%d')
        except:
            date = '1900-01-01'

        return date

    @staticmethod
    def clean_citation(pat_doc_num):
        """

        :param pat_doc_num:
        :return:
        """
        single_prefixes_to_ignore = ['D', 'H', 'T', 'X', 'R', 'W', 'S', 'L', 'F', 'P']
        double_prefixes_to_ignore = ['PP', 'RE', 'AI']
        pattern = re.compile('[^A-Za-z0-9]+')
        year_extract = re.compile('20[0-9][0-9]([0-9]+)')
        pat_doc_num = pat_doc_num.upper().replace('D. ', '')  # ensure uppercase
        pat_doc_num = pattern.sub('', pat_doc_num)  # Clean out non-alphanumeric
        prefixes = ["A1", "A2", "A9", "Bn", "B1", "B2", "Cn", "E1", "Fn", "H1", "I1",
                    "I2", "I3", "I4", "I5", "P1", "P2", "P3", "P4", "P9", "S1",
                    "H", "E", "S", "A", "B", "T"]
        kind_code_regex = re.compile('(%s)([0-9]{7})' % '|'.join(prefixes))
        # Edge case: they just put the work CITATION in:
        if pat_doc_num == 'CITATION':
            return ''
        # The next section handles all the cases of weird citation formatting
        # Do nothing. This is a utility
        if len(pat_doc_num) == 8 and pat_doc_num.isdigit():
            return pat_doc_num
        # This is a utility that is missing zeros
        elif len(pat_doc_num) < 8 and pat_doc_num.isdigit():
            return pat_doc_num.zfill(8)
        elif len(pat_doc_num) > 8 and pat_doc_num.isdigit():
            pat_doc_num = pat_doc_num.lstrip('0')  # extra zero for some reason
            if len(pat_doc_num) == 8:
                return pat_doc_num

            # Find the instances where a year is appended to a name
            ls = year_extract.findall(pat_doc_num)
            if len(ls) != 0:
                pat_doc_num = ls[0]
                return pat_doc_num.zfill(8)
            else:
                return None  # Failure. Send to human review
        else:
            # find instances that have kind codes prefixed with 7 numbers after
            ls = kind_code_regex.findall(pat_doc_num)
            if len(ls) != 0:
                if len(ls[0]) == 2:
                    pat_doc_num = ls[0][1]
                    kind = ls[0][0]
                    if kind in ['B', 'A', 'B1', 'B2']:
                        return pat_doc_num.zfill(8)
                    else:
                        return ''
            # These are design patents. Ignore
            if 'DES' in pat_doc_num:
                pat_doc_num = pat_doc_num.replace('DES', 'D')
            # Ignore certain prefixes
            if pat_doc_num[0] in single_prefixes_to_ignore or pat_doc_num[:2] in double_prefixes_to_ignore:
                return ''
            # If it is a utility, strip prefix, add zeros
            if pat_doc_num[0] == 'B' or pat_doc_num[0] == 'A':
                return pat_doc_num[:1].zfill(8)
            if pat_doc_num[-1] == 'B' or pat_doc_num[-1] == 'A':
                return pat_doc_num[:-1].zfill(8)

            return None  # Failure. Send to human review

    def determine_patent_type(self):
        """
        Determines patent version and parser
        :param file_name:
        :return:
        """

        if 'aps' in self.file_name:
            self.generation = 1
            self.parser = self.parse_v1
        elif 'ipg' in self.file_name:
            self.generation = 3
            self.parser = self.parse_v3
        elif 'pg' in self.file_name:
            self.generation = 2
            self.parser = self.parse_v2
        else:
            self.parser = self.parse_v2  # try parse v2
            # raise (Exception('Unknown format'))

    def parse_v1(self):
        """
        Parses v1 of the patent data.
        Credit goes to https://github.com/iamlemec/patents/blob/master/parse_grants.py#L18
        :return:
        """
        pat = None
        sec = None
        tag = None
        ipcver = None
        ignore = False
        for nline in chain(open(self.file_name, encoding='latin1', errors='ignore'), ['PATN']):
            # peek at next line
            (ntag, nbuf) = (nline[:4].rstrip(), nline[5:-1].rstrip())
            if tag is None:
                tag = ntag
                buf = nbuf
                continue
            if ntag == '':
                buf += nbuf
                continue

            # regular tags
            if tag == 'PATN':
                if pat is not None:
                    pat.ipcs = [(ipc, ipcver) for ipc in pat.ipcs]
                    if not ignore:
                        self.patents.append(pat)
                        if pat.citations:
                            self.citationpatents += 1
                    else:
                        ignore = False
                pat = Patent(self.file_name)
                self.totalpatents += 1

                sec = 'PATN'
            elif tag in ['INVT', 'ASSG', 'PRIR', 'CLAS', 'UREF', 'FREF', 'OREF', 'LREP', 'PCTA', 'ABST']:
                sec = tag
            elif tag in ['PAL', 'PAR', 'PAC', 'PA0', 'PA1']:
                if sec == 'ABST':
                    pat.abstract += '\n' + buf
            elif tag == 'WKU':
                if sec == 'PATN':
                    cleaned = self.clean_citation(buf)
                    if cleaned is None:
                        pat.flagged_for_review = True
                    elif cleaned.isdigit():
                        pat.patent_number = buf
                    # Malformed citation
                    else:
                        ignore = True  # Types of patents not to catch
            elif tag == 'ISD':
                if sec == 'PATN':
                    pat.grant_date = self.clean_date(buf)
            elif tag == 'APD':
                if sec == 'PATN':
                    pat.file_date = self.clean_date(buf)
            elif tag == 'ICL':
                if sec == 'CLAS':
                    pat.ipcs.append(buf)
            elif tag == 'TTL':
                if sec == 'PATN':
                    pat.title = buf
            elif tag == 'NAM':
                if sec == 'ASSG':
                    pat.owner = buf.upper()
            elif tag == 'STA':
                if sec == 'ASSG':
                    pat.country = 'US'
            elif tag == 'CNT':
                if sec == 'ASSG':
                    pat.country = buf[:2]
            elif tag == 'PNO':
                if sec == 'UREF':
                    cleaned = self.clean_citation(buf)
                    if cleaned is None:
                        pass
                    elif cleaned.isdigit():
                        pat.citations.append(cleaned)
                    else:
                        pass  # Types of patents not to catch

            # stage next tag and buf
            tag = ntag
            buf = nbuf

    def parse_v2(self):
        """
        Parses v2 of the patent data.
        """

        def parse(elem):
            patent = Patent(self.file_name)
            self.totalpatents += 1
            # top-level section
            bib = elem.find('SDOBI')

            # published patent
            patnum = self.get_child_text(bib.find('B100'), 'B110/DNUM/PDAT')
            cleaned = self.clean_citation(patnum)
            if cleaned is None:
                patent.flagged_for_review = True  # Malformed citation
                patent.patent_number = patnum
            elif cleaned.isdigit():
                patent.patent_number = cleaned
            else:
                return  # Ignore certain types of patents
            patent.grant_date = self.clean_date(self.get_child_text(bib.find('B100'), 'B140/DATE/PDAT'))
            patent.file_date = self.clean_date(self.get_child_text(bib.find('B200'), 'B220/DATE/PDAT'))

            # ipc code
            patref = bib.find('B500')
            ipcsec = patref.find('B510')
            ipcver = self.get_child_text(ipcsec, 'B516/PDAT')
            ipclist = []
            ipc1 = self.get_child_text(ipcsec, 'B511/PDAT')
            if ipc1 is not None:
                ipclist.append((ipc1, ipcver))
            for child in ipcsec.findall('B512'):
                ipclist.append((self.get_child_text(child, 'PDAT'), ipcver))
            patent.ipcs = ipclist

            # citations
            cites = []
            refs = patref.find('B560')
            if refs is not None:
                for cite in refs.findall('B561'):
                    citation = self.get_child_text(cite, 'PCIT/DOC/DNUM/PDAT')
                    cleaned = self.clean_citation(citation)
                    if cleaned is None:
                        pass
                    elif cleaned.isdigit():
                        cites.append(cleaned)
                    else:
                        pass  # Types of patents not to catch
                if cites:
                    self.citationpatents += 1
            patent.citations = cites

            # title
            patent.title = self.get_child_text(patref, 'B540/STEXT/PDAT')

            # applicant name and address
            ownref = bib.find('B700/B730/B731/PARTY-US')
            if ownref is not None:
                patent.owner = self.get_child_text(ownref, 'NAM/ONM/STEXT/PDAT').upper()
                address = ownref.find('ADR')
                if address is not None:
                    patent.country = self.get_child_text(address, 'CTRY/PDAT', default='US')

            # abstract
            abspars = elem.findall('SDOAB/BTEXT/PARA')
            if len(abspars) > 0:
                patent.abstract = '\n'.join([self.raw_text(e) for e in abspars])
            self.patents.append(patent)
            return True

        lines = []
        for line in open(self.file_name).readlines():
            if self.acceptable(line):
                lines.append(line)
        lines = ['<root>\n'] + lines + ['</root>\n']
        context = etree.iterparse(BytesIO(''.join(lines).encode('utf-8')), tag='PATDOC', events=['end'],
                                  recover=True)
        for (_, elem) in context:
            parse(elem)

    def parse_v3(self):
        """
        Parses v3 of the patent data
        """

        def parse(elem):
            self.totalpatents += 1
            patent = Patent(self.file_name)

            # top-level section
            bib = elem.find('us-bibliographic-data-grant')
            pubref = bib.find('publication-reference')
            appref = bib.find('application-reference')

            # published patent
            pubinfo = pubref.find('document-id')
            patnum = self.get_child_text(pubinfo, 'doc-number')
            cleaned = self.clean_citation(patnum)
            if cleaned is None:
                patent.flagged_for_review = True  # Malformed citation
                print("FLAGGED")
                patent.patent_number = patnum
            elif cleaned.isdigit():
                patent.patent_number = cleaned
            else:
                return  # Ignore certain types of patents

            patent.grant_date = self.clean_date(self.get_child_text(pubinfo, 'date'))
            patent.file_date = self.clean_date(self.get_child_text(appref, 'document-id/date'))
            patent.title = self.get_child_text(bib, 'invention-title')

            # ipc codes
            ipcsec = bib.find('classifications-ipcr')
            if ipcsec is not None:
                for ipc in ipcsec.findall('classification-ipcr'):
                    patent.ipcs.append(('%s%s%s%3s%s' % (self.get_child_text(ipc, 'section'),
                                                         self.get_child_text(ipc, 'class'),
                                                         self.get_child_text(ipc, 'subclass'),
                                                         self.get_child_text(ipc, 'main-group'),
                                                         self.get_child_text(ipc, 'subgroup')),
                                        self.get_child_text(ipc, 'ipc-version-indicator/date')))

            ipcsec = bib.find('classification-ipc')
            if ipcsec is not None:
                ipcver = self.get_child_text(ipcsec, 'edition')
                ipc0 = ipcsec.find('main-classification')
                for ipc in chain([ipc0], ipcsec.findall('further-classification')):
                    itxt = ipc.text
                    itxt = itxt[:4] + itxt[4:7].replace('0', ' ') + itxt[7:].replace('/', '')
                    patent.ipcs.append((itxt, ipcver))

            # citations
            refs = bib.find('references-cited')
            prefix = ''
            if refs is None:
                refs = bib.find('us-references-cited')
                prefix = 'us-'

            cites = []
            if refs is not None:
                for cite in refs.findall(prefix + 'citation'):
                    pcite = cite.find('patcit')
                    if pcite is not None:
                        docid = pcite.find('document-id')
                        pnum = self.get_child_text(docid, 'doc-number')
                        kind = self.get_child_text(docid, 'kind')
                        # Make sure the citation conforms with expected form
                        cleaned = self.clean_citation(pnum)
                        if cleaned is None:
                            pass
                        elif cleaned.isdigit():
                            cites.append(cleaned)
                        else:
                            pass  # Types of patents not to catch / citations to ignore
                if cites:
                    self.citationpatents += 1

            patent.citations = cites

            # applicant name and address
            assignee = bib.find('assignees/assignee/addressbook')
            if assignee is not None:
                patent.owner = self.get_child_text(assignee, 'orgname').upper()
                address = assignee.find('address')
                patent.country = self.get_child_text(address, 'country')

            # abstract
            abspar = elem.find('abstract')
            if abspar is not None:
                patent.abstract = self.raw_text(abspar, sep=' ')

            self.patents.append(patent)

        lines = []
        for line in open(self.file_name).readlines():
            if self.acceptable(line):
                lines.append(line)

        lines = ['<root>\n'] + lines + ['</root>\n']
        context = etree.iterparse(BytesIO(''.join(lines).encode('utf-8')), tag='us-patent-grant', events=['end'],
                                  recover=True)
        for (_, elem) in context:
            parse(elem)
