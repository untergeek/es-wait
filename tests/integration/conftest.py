# """Top-level conftest.py"""

# # pylint: disable=missing-function-docstring,redefined-outer-name,R0913
# import typing as t
# from os import environ, path
# from datetime import datetime, timezone
# import random
# import string
# import pytest
# from dotenv import load_dotenv
# from elasticsearch8.exceptions import NotFoundError
# from es_client import Builder
# from es_client.helpers.logging import set_logging

# LOCALREPO = 'testing'
# LOGLEVEL = 'DEBUG'


# @pytest.fixture(scope='session')
# def client():
#     """Return an Elasticsearch client"""
#     project_root = path.abspath(path.join(path.dirname(__file__), '..'))
#     envpath = path.join(project_root, '.env')
#     load_dotenv(dotenv_path=envpath)
#     host = environ.get('TEST_ES_SERVER', None)
#     user = environ.get('TEST_USER', None)
#     pswd = environ.get('TEST_PASS', None)
#     cacrt = environ.get('CA_CRT', None)
#     file = environ.get('ES_CLIENT_FILE', None)  # Path to es_client YAML config
#     repo = environ.get('TEST_ES_REPO', 'found-snapshots')
#     if file:
#         kwargs = {'configfile': file}
#     else:
#         kwargs = {
#             'configdict': {
#                 'elasticsearch': {
#                     'client': {'hosts': host, 'ca_certs': cacrt},
#                     'other_settings': {'username': user, 'password': pswd},
#                 }
#             }
#         }
#     set_logging({'loglevel': LOGLEVEL, 'blacklist': ['elastic_transport', 'urllib3']})
#     builder = Builder(**kwargs)
#     builder.connect()
#     if builder.client.license.get_trial_status()['eligible_to_start_trial']:
#         builder.client.license.post_start_trial(acknowledge=True)
#     # This is a contradiction that cannot exist...
#     if repo == 'found-snapshots' and host == 'https://127.0.0.1:9200' and not file:
#         # We'll make our own and set the ENV var
#         create_repository(builder.client, LOCALREPO)
#         environ['TEST_ES_REPO'] = LOCALREPO
#     return builder.client


# def create_repository(client, name: str) -> None:
#     """
#     PUT _snapshot/REPO_NAME
#     {
#         "type": "fs",
#         "settings": {
#             "location": "RELATIVE_PATH"
#         }
#     }
#     """
#     repobody = {'type': 'fs', 'settings': {'location': '/media'}}
#     client.snapshot.create_repository(name=name, repository=repobody, verify=False)


# def randomstr(length: int = 16, lowercase: bool = False):
#     """Generate a random string"""
#     letters = string.ascii_uppercase
#     if lowercase:
#         letters = string.ascii_lowercase
#     return str(''.join(random.choices(letters + string.digits, k=length)))


# @pytest.fixture(scope='class')
# def repo(client):
#     """Return the elasticsearch repository"""
#     name = environ.get('TEST_ES_REPO', 'found-snapshots')  # Going with Cloud default
#     if not repo:
#         return False
#     try:
#         client.snapshot.get_repository(name=name)
#     except NotFoundError:
#         return False
#     return name  # Return the repo name if it's online


# @pytest.fixture(scope='class')
# def settings(defaults, prefix, repo, uniq):
#     def _settings(
#         plan_type: t.Literal['data_stream', 'index'] = 'data_stream',
#         rollover_alias: bool = False,
#         ilm: t.Union[t.Dict, False] = False,
#         sstier: str = 'hot',
#     ):
#         return {
#             'type': plan_type,
#             'prefix': prefix,
#             'rollover_alias': rollover_alias,
#             'repository': repo,
#             'uniq': uniq,
#             'ilm': ilm,
#             'defaults': defaults(sstier),
#         }

#     return _settings


# @pytest.fixture(scope='class')
# def skip_no_repo(repo):
#     def _skip_no_repo(skip_it: bool):
#         if skip_it:
#             if not repo:
#                 pytest.skip('No snapshot repository', allow_module_level=True)

#     return _skip_no_repo


# @pytest.fixture(scope='class')
# def skip_localhost():
#     def _skip_localhost(skip_it: bool):
#         if skip_it:
#             host = environ.get('TEST_ES_SERVER')
#             file = environ.get('ES_CLIENT_FILE', None)  # Path to es_client config
#             repo = environ.get('TEST_ES_REPO')
#             if repo == LOCALREPO and host == 'https://127.0.0.1:9200' and not file:
#                 pytest.skip(
#                     'Local Docker test does not work with this test',
#                     allow_module_level=False,
#                 )

#     return _skip_localhost


# @pytest.fixture(scope='class')
# def ymd():
#     return datetime.now(timezone.utc).strftime('%Y.%m.%d')
