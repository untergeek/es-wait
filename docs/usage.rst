.. _usage:

Usage
=====

The es-wait package provides waiter classes to monitor Elasticsearch operations.
Each waiter polls the cluster state until the task completes, times out, or
encounters too many errors.

Basic Example: Cluster Health
-----------------------------

Wait for the cluster to reach a green status:

.. code-block:: python

   from es_wait import Health
   from elasticsearch8 import Elasticsearch

   client = Elasticsearch()
   waiter = Health(client, check_type="status")
   try:
       waiter.wait()
       print("Cluster is healthy!")
   except EsWaitTimeout as e:
       print(f"Timed out after {e.elapsed} seconds")

Waiting for Index Relocation
----------------------------

Wait for an index to finish relocating:

.. code-block:: python

   from es_wait import Relocate
   from elasticsearch8 import Elasticsearch

   client = Elasticsearch()
   relocator = Relocate(client, name="my-index")
   relocator.wait()

Waiting for Snapshot Completion
-------------------------------

Wait for a snapshot to complete:

.. code-block:: python

   from es_wait import Snapshot
   from elasticsearch8 import Elasticsearch

   client = Elasticsearch()
   snap = Snapshot(client, snapshot="my-snapshot", repository="my-repo")
   snap.wait()

See the :ref:`api` section for details on all waiter classes.
