action items:

- forestdb logging callback

be aware that:

- high times for in memory snapshot creation
- no max size

things to try:

- unit of prefix compression is chunksize default 8 size, try 4?
- async + auto commit
- understand configs like maxWriterLockProb and walThreshold?
- split store lock into smaller?
- instruments?

---------
cbgt:

- start with GOTRACEBACK=crash
- integrate with minidump