"""

Much of the parsing code was based loosely on https://github.com/iamlemec/patents/
"""
from lxml import etree
from itertools import chain
from io import BytesIO
from IPython import embed


def get_text(parent, tag, default=''):
    """
    Get the text of a child node
    :param parent:
    :param tag:
    :param default:
    :return:
    """
    child = parent.find(tag)
    return (child.text or default) if child is not None else default


def raw_text(par, sep=''):
    """
    Get all text of node
    :param par:
    :param sep:
    :return:
    """
    return sep.join(par.itertext()).strip()


def clear(elem):
    """
    Preserves the memory for a process
    :param elem:
    :return:
    """
    elem.clear()
    while elem.getprevious() is not None:
        del elem.getparent()[0]


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


def parse_v1(file_name):
    """
    Parses v1 of the patent data.
    Credit goes to https://github.com/iamlemec/patents/blob/master/parse_grants.py#L18
    TODO: Rewrite / optimize this parser
    :return:
    """
    pat = None
    sec = None
    tag = None
    ipcver = None
    patents = []
    for nline in chain(open(file_name, encoding='latin1', errors='ignore'), ['PATN']):
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
                pat['ipclist'] = [(ipc, ipcver) for ipc in pat['ipclist']]
                patents.append(pat)
            pat = {'gen': 1, 'ipclist': [], 'citlist': []}
            sec = 'PATN'
        elif tag in ['INVT', 'ASSG', 'PRIR', 'CLAS', 'UREF', 'FREF', 'OREF', 'LREP', 'PCTA', 'ABST']:
            sec = tag
        elif tag in ['PAL', 'PAR', 'PAC', 'PA0', 'PA1']:
            if sec == 'ABST':
                if 'abstract' not in pat:
                    pat['abstract'] = buf
                else:
                    pat['abstract'] += '\n' + buf
        elif tag == 'WKU':
            if sec == 'PATN':
                pat['patnum'] = buf
        elif tag == 'ISD':
            if sec == 'PATN':
                pat['grantdate'] = buf
        elif tag == 'APD':
            if sec == 'PATN':
                pat['filedate'] = buf
        elif tag == 'OCL':
            if sec == 'CLAS':
                pat['class'] = buf
        elif tag == 'ICL':
            if sec == 'CLAS':
                pat['ipclist'].append(buf)
        elif tag == 'EDF':
            if sec == 'CLAS':
                ipcver = buf
        elif tag == 'TTL':
            if sec == 'PATN':
                pat['title'] = buf
        elif tag == 'NCL':
            if sec == 'PATN':
                pat['claims'] = buf
        elif tag == 'NAM':
            if sec == 'ASSG':
                pat['owner'] = buf.upper()
        elif tag == 'CTY':
            if sec == 'ASSG':
                pat['city'] = buf.upper()
        elif tag == 'STA':
            if sec == 'ASSG':
                pat['state'] = buf
                pat['country'] = 'US'
        elif tag == 'CNT':
            if sec == 'ASSG':
                pat['country'] = buf[:2]
        elif tag == 'PNO':
            if sec == 'UREF':
                pat['citlist'].append(buf)

        # stage next tag and buf
        tag = ntag
        buf = nbuf

    return patents


def parse_v3(file_name):
    """
    Parses v3 of the patent data TODO: write tests for parsers
    :return:
    """
    lines = [line for line in open(file_name).readlines() if acceptable(line)]  # TODO: maybe optimize
    lines = ['<root>\n'] + lines + ['</root>\n']
    context = etree.iterparse(BytesIO(''.join(lines).encode('utf-8')), tag='us-patent-grant', events=['end'],
                              recover=True)
    patents = []

    for _, elem in context:
        patent = {'gen': 3}

        # top-level section
        bib = elem.find('us-bibliographic-data-grant')
        pubref = bib.find('publication-reference')
        appref = bib.find('application-reference')

        # published patent
        pubinfo = pubref.find('document-id')
        patent['patnum'] = get_text(pubinfo, 'doc-number')
        patent['grantdate'] = get_text(pubinfo, 'date')

        # filing date
        patent['filedate'] = get_text(appref, 'document-id/date')

        # title
        patent['title'] = get_text(bib, 'invention-title')

        # ipc code
        ipclist = []

        ipcsec = bib.find('classifications-ipcr')
        if ipcsec is not None:
            for ipc in ipcsec.findall('classification-ipcr'):
                ipclist.append(('%s%s%s%3s%s' % (get_text(ipc, 'section'),
                                                 get_text(ipc, 'class'),
                                                 get_text(ipc, 'subclass'),
                                                 get_text(ipc, 'main-group'),
                                                 get_text(ipc, 'subgroup')),
                                get_text(ipc, 'ipc-version-indicator/date')))

        ipcsec = bib.find('classification-ipc')
        if ipcsec is not None:
            ipcver = get_text(ipcsec, 'edition')
            ipc0 = ipcsec.find('main-classification')
            for ipc in chain([ipc0], ipcsec.findall('further-classification')):
                itxt = ipc.text
                itxt = itxt[:4] + itxt[4:7].replace('0', ' ') + itxt[7:].replace('/', '')
                ipclist.append((itxt, ipcver))

        patent['ipclist'] = ipclist

        # us class
        oclsec = bib.find('classification-national')
        if oclsec is not None:
            patent['class'] = get_text(oclsec, 'main-classification')

        # claims
        patent['claims'] = get_text(bib, 'number-of-claims')

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
                    pnum = get_text(docid, 'doc-number')
                    kind = get_text(docid, 'kind')
                    if kind == 'A' or kind.startswith('B'):
                        cites.append(pnum)
        patent['citlist'] = cites

        # applicant name and address
        assignee = bib.find('assignees/assignee/addressbook')
        if assignee is not None:
            patent['owner'] = get_text(assignee, 'orgname').upper()
            address = assignee.find('address')
            patent['city'] = get_text(address, 'city').upper()
            patent['state'] = get_text(address, 'state')
            patent['country'] = get_text(address, 'country')

        # abstract
        abspar = elem.find('abstract')
        if abspar is not None:
            patent['abstract'] = raw_text(abspar, sep=' ')

        patents.append(patent)

    return patents


def parse_v2(file_name):
    """
    Parses v2 of the patent data. TODO: Create this parser
    :return:
    """
    lines = [line for line in open(file_name).readlines() if acceptable(line)]  # TODO: maybe optimize
    lines = ['<root>\n'] + lines + ['</root>\n']
    context = etree.iterparse(BytesIO(''.join(lines).encode('utf-8')), tag='us-patent-grant', events=['end'],
                              recover=True)
    patents = []

    for _, elem in context:
        pat = {'gen': 2}

        # top-level section
        bib = elem.find('SDOBI')

        # published patent
        pubref = bib.find('B100')
        pat['patnum'] = get_text(pubref, 'B110/DNUM/PDAT')
        pat['grantdate'] = get_text(pubref, 'B140/DATE/PDAT')

        # filing date
        appref = bib.find('B200')
        pat['filedate'] = get_text(appref, 'B220/DATE/PDAT')

        # ipc code
        patref = bib.find('B500')
        ipcsec = patref.find('B510')
        ipcver = get_text(ipcsec, 'B516/PDAT')
        ipclist = []
        ipc1 = get_text(ipcsec, 'B511/PDAT')
        if ipc1 is not None:
            ipclist.append((ipc1, ipcver))
        for child in ipcsec.findall('B512'):
            ipc = get_text(child, 'PDAT')
            ipclist.append((ipc, ipcver))
        pat['ipclist'] = ipclist

        # us class
        pat['class'] = get_text(patref, 'B520/B521/PDAT')

        # citations
        cites = []
        refs = patref.find('B560')
        if refs is not None:
            for cite in refs.findall('B561'):
                pcit = get_text(cite, 'PCIT/DOC/DNUM/PDAT')
                cites.append(pcit)
        pat['citlist'] = cites

        # title
        pat['title'] = get_text(patref, 'B540/STEXT/PDAT')

        # claims
        pat['claims'] = get_text(patref, 'B570/B577/PDAT')

        # applicant name and address
        ownref = bib.find('B700/B730/B731/PARTY-US')
        if ownref is not None:
            pat['owner'] = get_text(ownref, 'NAM/ONM/STEXT/PDAT').upper()
            address = ownref.find('ADR')
            if address is not None:
                pat['city'] = get_text(address, 'CITY/PDAT').upper()
                pat['state'] = get_text(address, 'STATE/PDAT')
                pat['country'] = get_text(address, 'CTRY/PDAT', default='US')

        # abstract
        abspars = elem.findall('SDOAB/BTEXT/PARA')
        if len(abspars) > 0:
            pat['abstract'] = '\n'.join([raw_text(e) for e in abspars])
        patents.append(pat)
    return patents


def determine_patent_type(file_name):
    """
    Determines patent version and parser
    :param file_name:
    :return:
    """

    if 'aps' in file_name:
        gen = 1
        parser = parse_v1
    elif 'ipg' in file_name:
        gen = 3
        parser = parse_v3
    elif 'pg' in file_name:
        gen = 2
        parser = parse_v2
    else:
        raise (Exception('Unknown format'))

    return gen, parser
