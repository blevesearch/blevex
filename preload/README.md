### Preload

The `preload` KV store is a wrapper KV store which will first load a set of KV pairs from an external source prior to opening the KV store.

#### Preparing the KV pairs

Assuming you have an existing bleve index named search.bleve:

```
$ bleve_export search.bleve search.blexport
```

This creates a new file search.blexport which is a gzipped sequence of KV pairs.

#### Preloading a KV store with these KV pairs

Create a new in-memory index with the NewUsing() method as follows:

```
i, err := bleve.NewUsing(
  "",
  bleve.NewIndexMapping(),
  bleve.Config.DefaultIndexType,
  preload.Name,
  map[string]interface{}{
    "kvStoreName_actual": gtreap.Name,
    "preloadpath":        pathToBleveExport,
  })
  ```

#### Why?

Why would you want to use this?  Unfortunately, all of the KV stores supported by bleve either use the `syscall` or the `unsafe` package.  This means they aren't suitable for environments like Google App Engine.  By exporting the KV pairs of an existing bleve index into a simple format, we can then package them up, and preload them into a in-memory index.
