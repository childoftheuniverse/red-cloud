Red Cloud
=========

Red Cloud is a distributed database system implemented in the Go programming
language. It mostly follows the design of the earlier versions of Apache
Cassandra, but it is based on more modern technology (such as gRPC, etcd,
protocol buffers and Prometheus monitoring). Like Cassandra 1.x, it offers an
RPC interface to perform all kinds of operations, instead of having an SQL-like
language as the later implementations of Apache Cassandra do.

Unlike Cassandra, Red Cloud does not come with its own replication. Instead,
it relies on the underlying data store to be replicated. This can be achieved
by running Red Cloud on top of a replicated basic file system which only has
to be adapted to the filesystem interface in
<https://github.com/childoftheuniverse/filesystem>. The required functions for
the file system to be used for Red Cloud are:

 - Create/Delete
 - Append

Red Cloud is named in honor of Maȟpíya Lúta of the Oglala, who had the wisdom
of understanding the importance of continuity in a world full of trouble.
