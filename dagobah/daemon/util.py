""" Utility functions for the Dagobah daemon. """

def api_call(fn):
    """ Returns function result in API format if requested from an API
    endpoint """

    @wraps(fn)
    def wrapper(*args, **kwargs):
        result = fn(*args, **kwargs)

        if request and request.endpoint == fn.__name__:
            return jsonify(result=result,
                           status=200)
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

        elif value == bool:

            in_dict[key] = (True
                            if str(in_dict[key]).lower() == 'true'
                            else False)

        else:

            try:
                if key in in_dict:
                    in_dict[key] = value(in_dict[key])
            except ValueError:
                return False

    return True
