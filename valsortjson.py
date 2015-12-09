#!/usr/bin/python

import json

class ComplexEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, complex):
            return [obj.real, obj.imag]
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


class MyDictEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, dict):
            return "dict"
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)

