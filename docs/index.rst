.. es-wait documentation master file

``es_wait`` Documentation
=========================

A simple way to wait for entities in Elasticsearch to be in the expected state.

.. code-block:: python

   from elasticsearch8 import Elasticsearch
   from es_wait import Snapshot

   client = Elasticsearch()

   snap_check = Snapshot(client, snapshot='SNAPNAME', repository='REPONAME')
   snap_check.wait()

The above example will wait until the snapshot is completed.

License
-------

Copyright (c) 2024 Aaron Mildenstein

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contents
--------

.. toctree::
   classes
   exceptions
   defaults
   utils
   :maxdepth: 1

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
