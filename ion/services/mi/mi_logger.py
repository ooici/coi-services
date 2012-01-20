#! /usr/bin/env python

import logging

mi_logger = logging.getLogger('mi_logger')
mi_logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)-10s %(module)-25s %(process)-6d %(threadName)-15s - %(message)s')
handler.setFormatter(formatter)
mi_logger.addHandler(handler)
