package rocksdb

import "github.com/tecbot/gorocksdb"

func applyConfig(o *gorocksdb.Options, config map[string]interface{}) (
	*gorocksdb.Options, error) {

	cim, ok := config["create_if_missing"].(bool)
	if ok {
		o.SetCreateIfMissing(cim)
	}

	eie, ok := config["error_if_exists"].(bool)
	if ok {
		o.SetErrorIfExists(eie)
	}

	wbs, ok := config["write_buffer_size"].(float64)
	if ok {
		o.SetWriteBufferSize(int(wbs))
	}

	mof, ok := config["max_open_files"].(float64)
	if ok {
		o.SetMaxOpenFiles(int(mof))
	}

	tt, ok := config["total_threads"].(float64)
	if ok {
		o.IncreaseParallelism(int(tt))
	}

	// options in the block based table options object
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()

	lcc, ok := config["lru_cache_capacity"].(float64)
	if ok {
		c := gorocksdb.NewLRUCache(int(lcc))
		bbto.SetBlockCache(c)
	}

	bfbpk, ok := config["bloom_filter_bits_per_key"].(float64)
	if ok {
		bf := gorocksdb.NewBloomFilter(int(bfbpk))
		bbto.SetFilterPolicy(bf)
	}

	// set the block based table options
	o.SetBlockBasedTableFactory(bbto)

	return o, nil
}
