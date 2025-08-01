import logging

import pytest
import functools
import socket

import common.common_type as ct
import common.common_func as cf
from utils.util_log import test_log as log
from common.common_func import param_info
from check.param_check import ip_check, number_check
from config.log_config import log_config
from utils.util_pymilvus import get_milvus, gen_unique_str, gen_default_fields, gen_binary_default_fields
from pymilvus.orm.types import CONSISTENCY_STRONG

timeout = 60
dimension = 128
delete_timeout = 60


# add a fixture for all index?

