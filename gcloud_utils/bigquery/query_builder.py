from string import Template

class QueryBuilder(object):
    def __init__(self, file_or_query):
        self._file_or_query = file_or_query
        self._query = self._load_query()
    
    @property
    def query(self):
        return self._query

    def _load_query(self):            
        try:
            with open(self._file_or_query) as filebuffer:
                query = filebuffer.read().replace('\n', ' ').replace('\r', '')
        except IOError:
            query = self._file_or_query
        return query

    def with_vars(self, **kwargs):
        template = Template(self._query)
        self._query = template.safe_substitute(kwargs)