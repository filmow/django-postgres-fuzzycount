from distutils.version import LooseVersion

from django.conf import settings
from django.db import connections
from django.db.models import QuerySet, Manager

from django.core.cache import cache

import django
import hashlib

import logging
logger = logging.getLogger(__name__)


DJANGO_VERSION_GTE_19 = LooseVersion(django.get_version()) \
                        >= LooseVersion('1.9')

FUZZY_CACHE_ENABLED = getattr(settings, "FUZZY_CACHE_ENABLED", False)
FUZZY_CACHE_TIME = getattr(settings, "FUZZY_CACHE_TIME", 120)


class FuzzyCountQuerySet(QuerySet):
    def count(self, nocache=False):
        postgres_engines = ("postgis", "postgresql", "django_postgrespool")
        engine = settings.DATABASES[self.db]["ENGINE"].split(".")[-1]
        is_postgres = engine.startswith(postgres_engines)

        # In Django 1.9 the query.having property was removed and the
        # query.where property will be truthy if either where or having
        # clauses are present. In earlier versions these were two separate
        # properties query.where and query.having
        if DJANGO_VERSION_GTE_19:
            is_filtered = self.query.where
        else:
            is_filtered = self.query.where or self.query.having

        if not is_postgres or is_filtered:
            # Get the count or execute count() and save in cache
            if FUZZY_CACHE_ENABLED and not nocache:
                query_str = self.query.__str__()
                qs_key = "FUZZY_{}".format(
                    hashlib.md5(query_str).hexdigest())
                count_cache = cache.get(qs_key)

                if not count_cache:
                    count = super(FuzzyCountQuerySet, self).count()
                    count_cache = cache.set(qs_key, count, FUZZY_CACHE_TIME)
                    if settings.DEBUG:
                        logger.debug(
                            "Creating cache for count: {}".format(query_str))
                return count_cache

            # Cache disabled
            return super(FuzzyCountQuerySet, self).count()

        cursor = connections[self.db].cursor()
        cursor.execute("SELECT reltuples FROM pg_class "
                       "WHERE relname = '%s';" % self.model._meta.db_table)
        return int(cursor.fetchone()[0])


FuzzyCountManager = Manager.from_queryset(FuzzyCountQuerySet)
