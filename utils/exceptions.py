class TypeErrorOrEmptyListException(Exception):
    def __init___(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)