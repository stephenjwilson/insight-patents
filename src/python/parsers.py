"""

Much of the parsing code was based loosely on https://github.com/iamlemec/patents/
"""
from itertools import chain

from lxml import etree

# try:
from src.python.patent import Patent


#except:
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
        # Passed Parameters
        self.file_name = file_name

        # Other Parameters - to be defined
        self.generation = None
        self.parser = self.parse_v3  # default is v3 XML
        self.patents = []
        if detect_format:
            # Check file format / generation
            self.determine_patent_type()

        # Parse the data into a list of patents
        self.parser()

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

    def handle_all(self, pp, parsing_func):
        for (_, pat) in pp.read_events():
            if not parsing_func(pat):
                return False
            self.clear(pat)
        return True

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
        else:
            return True

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
            raise (Exception('Unknown format'))

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
                    self.patents.append(pat)
                pat = Patent(self.file_name)
                citations = []

                sec = 'PATN'
            elif tag in ['INVT', 'ASSG', 'PRIR', 'CLAS', 'UREF', 'FREF', 'OREF', 'LREP', 'PCTA', 'ABST']:
                sec = tag
            elif tag in ['PAL', 'PAR', 'PAC', 'PA0', 'PA1']:
                if sec == 'ABST':
                    pat.abstract += '\n' + buf
            elif tag == 'WKU':
                if sec == 'PATN':
                    pat.patent_number = buf
            elif tag == 'ISD':
                if sec == 'PATN':
                    pat.grant_date = buf
            elif tag == 'APD':
                if sec == 'PATN':
                    pat.file_date = buf
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
                    pat.citations.append(buf)

            # stage next tag and buf
            tag = ntag
            buf = nbuf

    def parse_v2(self):
        """
        Parses v2 of the patent data.
        """

        # Optimize

        # lines = [line for line in open(self.file_name).readlines() if self.acceptable(line)]  # TODO: maybe optimize
        # lines = ['<root>\n'] + lines + ['</root>\n']
        # context = etree.iterparse(BytesIO(''.join(lines).encode('utf-8')), tag='us-patent-grant', events=['end'],
        #                           recover=True)
        def parse(elem):
            patent = Patent(self.file_name)
            # top-level section
            bib = elem.find('SDOBI')

            # published patent
            patent.patent_number = self.get_child_text(bib.find('B100'), 'B110/DNUM/PDAT')
            patent.grant_date = self.get_child_text(bib.find('B100'), 'B140/DATE/PDAT')
            patent.file_date = self.get_child_text(bib.find('B200'), 'B220/DATE/PDAT')

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
                    cites.append(self.get_child_text(cite, 'PCIT/DOC/DNUM/PDAT'))
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

        pp = etree.XMLPullParser(tag='us-patent-grant', events=['end'], recover=True)
        with open(self.file_name, errors='ignore') as f:
            pp.feed('<root>\n')
            for line in f:
                if line.startswith('<?xml'):
                    if not self.handle_all(pp, parse):
                        break
                elif line.startswith('<!DOCTYPE'):
                    pass
                else:
                    pp.feed(line)
            else:
                pp.feed('</root>\n')
                self.handle_all(pp, parse)

    def parse_v3(self):
        """
        Parses v3 of the patent data TODO: write tests for parsers
        """

        # lines = [line for line in open(self.file_name).readlines() if self.acceptable(line)]  # TODO: maybe optimize
        # lines = ['<root>\n'] + lines + ['</root>\n']
        # context = etree.iterparse(BytesIO(''.join(lines).encode('utf-8')), tag='us-patent-grant', events=['end'],
        #                           recover=True)

        def parse(elem):
            patent = Patent(self.file_name)

            # top-level section
            bib = elem.find('us-bibliographic-data-grant')
            pubref = bib.find('publication-reference')
            appref = bib.find('application-reference')

            # published patent
            pubinfo = pubref.find('document-id')
            patent.patent_number = self.get_child_text(pubinfo, 'doc-number')
            patent.grant_date = self.get_child_text(pubinfo, 'date')
            patent.file_date = self.get_child_text(appref, 'document-id/date')
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
                        if kind == 'A' or kind.startswith('B'):
                            cites.append(pnum)
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

        pp = etree.XMLPullParser(tag='us-patent-grant', events=['end'], recover=True)
        with open(self.file_name, errors='ignore') as f:
            pp.feed('<root>\n')
            for line in f:
                if line.startswith('<?xml'):
                    if not self.handle_all(pp, parse):
                        break
                elif line.startswith('<!DOCTYPE'):
                    pass
                else:
                    pp.feed(line)
            else:
                pp.feed('</root>\n')
                self.handle_all(pp, parse)
