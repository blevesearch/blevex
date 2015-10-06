action items:

- forestdb logging callback

be aware that:

- high times for in memory snapshot creation
- no max size

things to try:

- unit of prefix compressiion is chunksize default 8size, try 4?
- async + auto commit

---------
cbgt
- start with GOTRACEBACK=crash
 - integrate with minidump