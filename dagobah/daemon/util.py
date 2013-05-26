""" Utility functions for the Dagobah daemon. """

from datetime import date, datetime

from flask import request, json, Response, abort
from functools import wraps

try:
    from pymongo.objectid import ObjectId
except ImportError:
    from bson import ObjectId

from dagobah.core import DagobahError


class DagobahEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        elif isinstance(obj, datetime) or isinstance(obj, date):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


def jsonify(*args, **kwargs):
    return Response(json.dumps(dict(*args, **kwargs),
                               cls=DagobahEncoder,
                               indent=2),
                    mimetype='application/json')


def api_call(fn):
    """ Returns function result in API format if requested from an API
    endpoint """

    @wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            result = fn(*args, **kwargs)
        except DagobahError:
            if request and request.endpoint == fn.__name__:
                abort(400)
            raise
        except Exception as e:
            print e
            raise

        if request and request.endpoint == fn.__name__:
            status_code = None
            try:
                if result and '_status' in result:
                    status_code = result['_status']
                    del result['_status']
            except TypeError:
                pass

            if isinstance(result, dict):
                if 'result' in result:
                    return jsonify(status=status_code if status_code else 200,
                                   **result)
                else:
                    return jsonify(status=status_code if status_code else 200,
                                   result=result)
            else:
                return jsonify(status=status_code if status_code else 200,
                               result=result)

        else:
            return result

    return wrapper


def validate_dict(in_dict, **kwargs):
    """ Returns Boolean of whether given dict conforms to type specifications
    given in kwargs. """

    if not isinstance(in_dict, dict):
        raise ValueError('requires a dictionary')

    for key, value in kwargs.iteritems():

        if key == 'required':
            for required_key in value:
                if required_key not in in_dict:
                    return False

        elif key not in in_dict:
            continue

        elif value == bool:

            in_dict[key] = (True
                            if str(in_dict[key]).lower() == 'true'
                            else False)

        else:

            if (isinstance(in_dict[key], list) and
                len(in_dict[key]) == 1 and
                value != list):
                in_dict[key] = in_dict[key][0]

            try:
                if key in in_dict:
                    in_dict[key] = value(in_dict[key])
            except ValueError:
                return False

    return True
