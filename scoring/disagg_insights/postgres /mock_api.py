from flask import Flask, Response, stream_with_context
import time
import uuid
import datetime
import random

APP = Flask(__name__)


# int, time stamp, time stamp, uuid, double precision, double precision
@APP.route("/very_large_request/<int:rowcount>", methods=["GET"])
def get_large_request(rowcount):
    """retunrs N rows of data"""

    def f():
        """The generator of mock data"""
        # date = datetime.datetime(2013, 9, 20, 13, 00)
        # date = datetime.datetime(2013, 9, 30, 23, 00)
        date = datetime.datetime(2013, 10, 11, 9, 00)

        for _i in range(rowcount):
            # time.sleep(.01)
            data_id = random.randint(0, 1000)
            value = random.randint(-1000, 1000000)

            yield f"('{data_id}', '{date}', {value})\n"
            date += datetime.timedelta(minutes=15)

    return Response(stream_with_context(f()))


APP.run()
