"""This is the main JSON handler. It converts objects to their correct representation.

Currently it only changes datetime objects in to milliseconds since epoch, and there's a function
to revert back to datetime objects as well."""

import datetime
import time
import json

__author__ = "Sebastiaan"
__version__ = "0.2"

TIME_OFFSET = datetime.datetime.utcnow() - datetime.datetime.now()

try:
	import cjson
	has_cjson = True
except:
	has_cjson = False

def dumps(obj):
    """Dump an object to a json string"""
    return json.dumps(obj, default=_handler)

def loads(obj):
    """Load a string in to a python object"""
    return json.loads(obj)

def cdumps(obj):
	"""Same thing as dumps(), but much faster if cjson is installed. Does not support datetimes!!!!"""
	if has_cjson:
		return cjson.encode(obj)
	else:
		return dumps(obj)

def cloads(obj):
	"""Same thing as loads(), but much faster if cjson is installed. Does not support datetimes!!!!"""
	if has_cjson:
		return cjson.decode(obj)
	else:
		return loads(obj)

def _handler(obj):
    if isinstance(obj, datetime.datetime):
        try:
            return int(time.mktime(obj.timetuple()) * 1000 + obj.microsecond / 1000)
        except:
            epoch = datetime.datetime(1970, 1, 1)
            td = obj - epoch
            # For Python 2.7+
            #return int(td.total_seconds()*1000)
            # For Python 2.6
            return int((td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 10**3)

    elif hasattr(obj, 'isoformat'):
        return obj.isoformat()
    else:
        raise TypeError, 'Object of type %s with value of %s is not JSON serializable' % (type(obj), repr(obj))

def millisec_to_datetime(millis):
    """Convert milliseconds to epoch to Python datetime objects.
    There's some loss in this function, but the loss is less than 1 second."""
    try:
        return datetime.datetime.fromtimestamp(millis/1000) + datetime.timedelta(milliseconds=millis%1000)
    except:
        return datetime.datetime(1970, 1, 1) - TIME_OFFSET + datetime.timedelta(milliseconds=int(millis))

if __name__ == "__main__":
	import logging, json
	logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
	logging.info("Testing the main json handler used at Skout")
	test_1 = {	'name': 'abc',
				'birthday': datetime.datetime.utcnow() - datetime.timedelta(days=365*23)}
	logging.info("Test #1: [%s]", test_1)
	test_1_json = json.dumps(test_1, default=_handler)
	logging.info("JSON: [%s]", test_1_json)
	logging.info("Back to python: [%s]", json.loads(test_1_json))

	logging.info("Now we have the datetime in millis. Revert to datetime...")
	logging.info(millisec_to_datetime(json.loads(test_1_json)['birthday']))
	assert abs(millisec_to_datetime(json.loads(test_1_json)['birthday']) - test_1['birthday']) < datetime.timedelta(seconds = 1)

	logging.info("Now testing cdumps and cjson!")

	test_2 = {'hello': 1, 'bleh': 2}
	temp = cdumps(test_2)
	logging.info("%s encoded as %s with cjson", test_2, temp)
	temp_compare = dumps(test_2)
	assert temp_compare == temp, "cdumps and dumps don't give exactly the same output"
	test_2_back = cloads(temp)
	logging.info("%s decoded as %s", temp, test_2_back)
	assert test_2 == test_2_back, "Oh no! before/after not the same."

	logging.info("cdumps works.")
