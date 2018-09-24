"""
The Patent class, which contains the fields of interest for storing data from XML
"""
import datetime
import re


class Patent(object):
    """
    The Patent class, which contains the fields of interest for storing data from XML
    """

    def __init__(self, source_file_name, patent_number=None, file_date=None, title=None, grant_date=None,
                 abstract="", owner=None, country=None, ipcs=None, citations=None, chemicals=None):
        """
        The initialization method of Patent, which requires the name of the file the patent is from,
        optionally takes more parameters.
        :param source_file_name: The name of the file that contained the patent
        :param patent_number: The patent number from the USTPO
        :param file_date: The data the patent was filed
        :param title: The title of the patent
        :param grant_date: The date the patent was granted
        :param abstract: The abstract of the patent
        :param owner: The owner of the patent
        :param country: The country of origin
        :param ipcs: A list of strings representing the International Patent Classification
        :param citations: A list of strings representing the patent numbers of cited patents
        :param chemicals: A list of chemicals referenced in the patent

        """
        self.file_name = source_file_name
        self.patent_number = patent_number
        self.file_date = file_date
        self.title = title
        self.grant_date = grant_date
        self.abstract = abstract
        self.owner = owner
        self.country = country
        self.ipcs = ipcs
        self.citations = citations
        self.chemicals = chemicals
        if self.ipcs is None:
            self.ipcs = []
        if self.citations is None:
            self.citations = []
        if self.chemicals is None:
            self.chemicals = []

        # TODO: make custom get or setters?
        # http://code.activestate.com/recipes/307969-generating-getset-methods-using-a-metaclass/
        # https://github.com/scikit-learn/scikit-learn/blob/f0ab589f/sklearn/base.py#L176

    def to_csv(self, fields, sep=','):
        """
        Formats the patent properties for csv file output
        :param fields: the list of fields (strings) to output
        :param sep: The separator of fields. Default is ','
        :return: a string containing the csv formatted data
        """
        s = []
        # Deal with all other fields
        pattern = re.compile('[^A-Za-z0-9_\. ]+')
        for field in fields:
            val = getattr(self, field)
            if val is not None:  # Silently ignores fields that are not present
                if type(val) == str:
                    val = val.replace('"', '').replace(",", '')
                    val = pattern.sub('', val)

                    if 'date' in field:
                        val = datetime.datetime.strptime(str(val), '%Y%m%d').strftime('%Y-%m-%d')
                elif type(val) == list:
                    if 'ipc' in field:
                        # Deal with ipcs
                        val = '|'.join(["{}-{}".format(ipc[0], ipc[1]) for ipc in val])
                    elif 'citations' == field:
                        val = "|".join(val)
                    elif 'chemicals' == field:
                        val = "|".join(val)
                val = '"%s"' % val
                s.append(val.strip())
            else:
                s.append('N/A')
        return sep.join(s) + '\n'

    def to_neo4j_relationships(self, sep=','):
        """
        Formats neo4j relationships
        :param sep: The separator of fields. Default is ','
        :return: A formatted string for this patent
        """
        s = []
        patent_number = self.patent_number
        for citation in self.citations:
            s.append(patent_number + sep + citation + '\n')
        return ''.join(s)
