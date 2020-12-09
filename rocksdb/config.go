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

	pc, ok := config["paranoid_checks"].(bool)
	if ok {
		o.SetParanoidChecks(pc)
	}

	ill, ok := config["info_log_level"].(int)
	if ok {
		o.SetInfoLogLevel(gorocksdb.InfoLogLevel(uint(ill)))
	}

	tt, ok := config["total_threads"].(int)
	if ok {
		o.IncreaseParallelism(tt)
	}

	ofpl, ok := config["optimize_for_point_lookup"].(int)
	if ok {
		o.OptimizeForPointLookup(uint64(ofpl))
	}

	olsc, ok := config["optimize_level_style_compaction"].(int)
	if ok {
		o.OptimizeLevelStyleCompaction(uint64(olsc))
	}

	ousc, ok := config["optimize_universal_style_compaction"].(int)
	if ok {
		o.OptimizeUniversalStyleCompaction(uint64(ousc))
	}

	wbs, ok := config["write_buffer_size"].(int)
	if ok {
		o.SetWriteBufferSize(wbs)
	}

	mwbn, ok := config["max_write_buffer_number"].(int)
	if ok {
		o.SetMaxWriteBufferNumber(mwbn)
	}

	mwbntm, ok := config["min_write_buffer_number_to_merge"].(int)
	if ok {
		o.SetMinWriteBufferNumberToMerge(mwbntm)
	}

	mof, ok := config["max_open_files"].(int)
	if ok {
		o.SetMaxOpenFiles(mof)
	}

	c, ok := config["compression"].(uint)
	if ok {
		o.SetCompression(gorocksdb.CompressionType(c))
	}

	mltc, ok := config["min_level_to_compress"].(int)
	if ok {
		o.SetMinLevelToCompress(mltc)
	}

	nl, ok := config["num_levels"].(int)
	if ok {
		o.SetNumLevels(nl)
	}

	lfnct, ok := config["level0_file_num_compaction_trigger"].(int)
	if ok {
		o.SetLevel0FileNumCompactionTrigger(lfnct)
	}

	lswt, ok := config["level0_slowdown_writes_trigger"].(int)
	if ok {
		o.SetLevel0SlowdownWritesTrigger(lswt)
	}

	lstopwt, ok := config["level0_stop_writes_trigger"].(int)
	if ok {
		o.SetLevel0StopWritesTrigger(lstopwt)
	}

	mmcl, ok := config["max_mem_compaction_level"].(int)
	if ok {
		o.SetMaxMemCompactionLevel(mmcl)
	}

	tfsb, ok := config["target_file_size_base"].(int)
	if ok {
		o.SetTargetFileSizeBase(uint64(tfsb))
	}

	tfsm, ok := config["target_file_size_multiplier"].(int)
	if ok {
		o.SetTargetFileSizeMultiplier(tfsm)
	}

	mbflb, ok := config["max_bytes_for_level_base"].(int)
	if ok {
		o.SetMaxBytesForLevelBase(uint64(mbflb))
	}

	mbflm, ok := config["max_bytes_for_level_multiplier"].(float64)
	if ok {
		o.SetMaxBytesForLevelMultiplier(mbflm)
	}

	uf, ok := config["use_fsync"].(bool)
	if ok {
		o.SetUseFsync(uf)
	}

	dofpm, ok := config["delete_obsolete_files_period_micros"].(int)
	if ok {
		o.SetDeleteObsoleteFilesPeriodMicros(uint64(dofpm))
	}

	mbc, ok := config["max_background_compactions"].(int)
	if ok {
		o.SetMaxBackgroundCompactions(mbc)
	}

	mbf, ok := config["max_background_flushes"].(int)
	if ok {
		o.SetMaxBackgroundFlushes(mbf)
	}

	mlfs, ok := config["max_log_file_size"].(int)
	if ok {
		o.SetMaxLogFileSize(mlfs)
	}

	lfttr, ok := config["log_file_time_to_roll"].(int)
	if ok {
		o.SetLogFileTimeToRoll(lfttr)
	}

	klfn, ok := config["keep_log_file_num"].(int)
	if ok {
		o.SetKeepLogFileNum(klfn)
	}

	hrl, ok := config["hard_rate_limit"].(float64)
	if ok {
		o.SetHardRateLimit(hrl)
	}

	rldmm, ok := config["rate_limit_delay_max_millisecond"].(int)
	if ok {
		o.SetRateLimitDelayMaxMilliseconds(uint(rldmm))
	}

	mmfs, ok := config["max_manifest_file_size"].(int)
	if ok {
		o.SetMaxManifestFileSize(uint64(mmfs))
	}

	tcnsb, ok := config["table_cache_numshardbits"].(int)
	if ok {
		o.SetTableCacheNumshardbits(tcnsb)
	}

	tcrscl, ok := config["table_cache_remove_scan_count_limit"].(int)
	if ok {
		o.SetTableCacheRemoveScanCountLimit(tcrscl)
	}

	abs, ok := config["arena_block_size"].(int)
	if ok {
		o.SetArenaBlockSize(abs)
	}

	dac, ok := config["disable_auto_compactions"].(bool)
	if ok {
		o.SetDisableAutoCompactions(dac)
	}

	wts, ok := config["WAL_ttl_seconds"].(int)
	if ok {
		o.SetWALTtlSeconds(uint64(wts))
	}

	wslm, ok := config["WAL_size_limit_MB"].(int)
	if ok {
		o.SetWalSizeLimitMb(uint64(wslm))
	}

	mps, ok := config["manifest_preallocation_size"].(int)
	if ok {
		o.SetManifestPreallocationSize(mps)
	}

	prkwf, ok := config["purge_redundant_kvs_while_flush"].(bool)
	if ok {
		o.SetPurgeRedundantKvsWhileFlush(prkwf)
	}

	amr, ok := config["allow_mmap_reads"].(bool)
	if ok {
		o.SetAllowMmapReads(amr)
	}

	amw, ok := config["allow_mmap_writes"].(bool)
	if ok {
		o.SetAllowMmapWrites(amw)
	}

	sleor, ok := config["skip_log_error_on_recovery"].(bool)
	if ok {
		o.SetSkipLogErrorOnRecovery(sleor)
	}

	sdps, ok := config["stats_dump_period_sec"].(int)
	if ok {
		o.SetStatsDumpPeriodSec(uint(sdps))
	}

	aroo, ok := config["advise_random_on_open"].(bool)
	if ok {
		o.SetAdviseRandomOnOpen(aroo)
	}

	ahocs, ok := config["access_hint_on_compaction_start"].(int)
	if ok {
		o.SetAccessHintOnCompactionStart(gorocksdb.CompactionAccessPattern(uint(ahocs)))
	}

	uam, ok := config["use_adaptive_mutex"].(bool)
	if ok {
		o.SetUseAdaptiveMutex(uam)
	}

	bps, ok := config["bytes_per_sync"].(int)
	if ok {
		o.SetBytesPerSync(uint64(bps))
	}

	cs, ok := config["compaction_style"].(int)
	if ok {
		o.SetCompactionStyle(gorocksdb.CompactionStyle(uint(cs)))
	}

	mssii, ok := config["max_sequential_skip_in_iterations"].(int)
	if ok {
		o.SetMaxSequentialSkipInIterations(uint64(mssii))
	}

	ius, ok := config["inplace_update_support"].(bool)
	if ok {
		o.SetInplaceUpdateSupport(ius)
	}

	iunl, ok := config["inplace_update_num_locks"].(int)
	if ok {
		o.SetInplaceUpdateNumLocks(iunl)
	}

	es, ok := config["enable_statistics"].(bool)
	if ok && es {
		o.EnableStatistics()
	}

	pfbl, ok := config["prepare_for_bulk_load"].(bool)
	if ok && pfbl {
		o.PrepareForBulkLoad()
	}

	// options in the block based table options object
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()

	lcc, ok := config["lru_cache_capacity"].(int)
	if ok {
		c := gorocksdb.NewLRUCache(lcc)
		bbto.SetBlockCache(c)
	}

	bfbpk, ok := config["bloom_filter_bits_per_key"].(int)
	if ok {
		bf := gorocksdb.NewBloomFilter(bfbpk)
		bbto.SetFilterPolicy(bf)
	}

	// set the block based table options
	o.SetBlockBasedTableFactory(bbto)

	return o, nil
}

func (s *Store) newWriteOptions() *gorocksdb.WriteOptions {
	wo := gorocksdb.NewDefaultWriteOptions()

	if s.woptSyncUse {
		wo.SetSync(s.woptSync)
	} else {
		// request fsync on write for safety by default
		wo.SetSync(true)
	}
	if s.woptDisableWALUse {
		wo.DisableWAL(s.woptDisableWAL)
	}

	return wo
}

func (s *Store) newReadOptions() *gorocksdb.ReadOptions {
	ro := gorocksdb.NewDefaultReadOptions()

	if s.roptVerifyChecksumsUse {
		ro.SetVerifyChecksums(s.roptVerifyChecksums)
	}
	if s.roptFillCacheUse {
		ro.SetFillCache(s.roptFillCache)
	}
	if s.roptReadTierUse {
		ro.SetReadTier(gorocksdb.ReadTier(s.roptReadTier))
	}

	return ro
}
