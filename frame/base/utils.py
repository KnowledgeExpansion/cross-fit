def iteritems(d):
    return d.iteritems()

def enum(*sequential, **named):
    enums = dict(list(zip(sequential, list(range(0, len(sequential))))), **named)
    reverse = dict((value, key) for key, value in iteritems(enums))
    enums['reverse_mapping'] = reverse
    enums['count'] = len(reverse)
    return type('Enum', (), enums)


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]