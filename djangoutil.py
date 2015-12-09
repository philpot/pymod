#!/usr/bin/env python

import sys
from django.db.models.manager import Manager

def show(dbInstance, file=sys.stdout):
    cls = dbInstance.__class__
    print(dbInstance, file=file)
    for field in cls._meta.get_fields():
        val = None
        try:
            accessor = field.get_accessor_name()
        except AttributeError:
            accessor = field.name
        val = getattr(dbInstance, accessor)
        if isinstance(val, Manager):
            print("{}:\t{}".format(accessor, val.all()), file=file)
        else:
            print("{}:\t{}".format(accessor, val), file=file)



