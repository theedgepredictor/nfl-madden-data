import random
import time


def pfr_request(url, session=None):
    """
    PFR requests will require a session to avoid rate limiting.
    Be polite to their server
    """
    rand_int_sleep = random.randint(1, 8)
    rand_float_sleep = round(random.random(), 2)
    print(f"Schleeping for {rand_int_sleep + rand_float_sleep}")
    time.sleep(rand_int_sleep + rand_float_sleep)
    if session:
        r = session.get(url)
    else:
        raise Exception('No session passed. PFR requests will require a session to avoid rate limiting.')
    return r