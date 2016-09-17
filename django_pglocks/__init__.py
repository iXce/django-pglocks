from zlib import crc32
from django.utils import six

__version__ = '1.0.2'

class advisory_lock(object):

    def __init__(self, lock_id, shared=False, wait=True, using=None, connection=None):
        self.lock_id = lock_id
        self.shared = shared
        self.wait = wait

        # The `connection` can set to an instance of :class:`psycopg2.connection`.
        # If missing then the connection is retrieved with Django.

        if connection:
            assert not using
        else:
            from django.db import DEFAULT_DB_ALIAS, connections
            if using is None:
                using = DEFAULT_DB_ALIAS
            connection = connections[using]

        self.using = using
        self.connection = connection
        self.cursor = None

        # Assemble the function name based on the options.
        function_name = 'pg_'

        if not self.wait:
            function_name += 'try_'

        function_name += 'advisory_lock'

        if self.shared:
            function_name += '_shared'

        release_function_name = 'pg_advisory_unlock'
        if self.shared:
            release_function_name += '_shared'

        # Format up the parameters.

        tuple_format = False

        if isinstance(lock_id, (list, tuple,)):
            if len(lock_id) != 2:
                raise ValueError("Tuples and lists as lock IDs must have exactly two entries.")

            if not isinstance(lock_id[0], six.integer_types) or not isinstance(lock_id[1], six.integer_types):
                raise ValueError("Both members of a tuple/list lock ID must be integers")

            tuple_format = True
        elif isinstance(lock_id, six.string_types):
            # Generates an id within postgres integer range (-2^31 to 2^31 - 1).
            # crc32 generates an unsigned integer in Py3, we convert it into
            # a signed integer using 2's complement (this is a noop in Py2)
            pos = crc32(lock_id.encode("utf-8"))
            lock_id = (2 ** 31 - 1) & pos
            if pos & 2 ** 31:
                lock_id -= 2 ** 31
        elif not isinstance(lock_id, six.integer_types):
            raise ValueError("Cannot use %s as a lock id" % lock_id)

        if tuple_format:
            base = "SELECT %s(%d, %d)"
            params = (lock_id[0], lock_id[1],)
        else:
            base = "SELECT %s(%d)"
            params = (lock_id,)

        self.query_base = base
        self.acquire_params = (function_name, ) + params
        self.release_params = (release_function_name, ) + params

    def acquire(self):
        command = self.query_base % self.acquire_params
        self.cursor = self.connection.cursor()

        self.cursor.execute(command)

        if not self.wait:
            self.acquired = self.cursor.fetchone()[0]
        else:
            self.acquired = True
        return self.acquired

    def release(self):
        if self.acquired:
            command = self.query_base % self.release_params
            self.cursor.execute(command)
            self.acquired = False
        self.cursor.close()

    def __enter__(self):
        return self.acquire()

    def __exit__(self):
        return self.release()
