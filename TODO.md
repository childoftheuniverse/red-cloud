Functionality
=============

The following functionality is still missing from red-cloud:

 * Tablet splits and joins
 * TTLs
 * Batch mutations
 * Streaming inserts
 * Monitoring metrics
 * Authentication
 * Authorization

Bugs
====

The following is a list of known bugs in the current implementation of the
database engine:

Alive/Dead Handling
-------------------

 * Sometimes, nodes are not added properly but ignored instead.
 * Nodes restarted on the same port don't get ServeRange requests and thus
   never start serving tablets assigned to them.
 * When nodes enter the dead state, their tablets don't get reassigned
   immediately.

Small Bugs
----------

 * Newly created tables refuse inserts with "Tablet not loaded".
 * DeleteTable does not delete tablet data.
 * DeleteTable RPCs lead to logsorting crashes.
