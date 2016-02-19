package rocksdb

// #include "rocksdb/c.h"
import "C"

func applyConfig(o *C.rocksdb_options_t, config map[string]interface{}) (
	*C.rocksdb_options_t, error) {

	cim, ok := config["create_if_missing"].(bool)
	if ok {
		C.rocksdb_options_set_create_if_missing(o, boolToChar(cim))
	}

	eie, ok := config["error_if_exists"].(bool)
	if ok {
		C.rocksdb_options_set_error_if_exists(o, boolToChar(eie))
	}

	pc, ok := config["paranoid_checks"].(bool)
	if ok {
		C.rocksdb_options_set_paranoid_checks(o, boolToChar(pc))
	}

	ill, ok := config["info_log_level"].(float64)
	if ok {
		C.rocksdb_options_set_info_log_level(o, C.int(ill))
	}

	tt, ok := config["total_threads"].(float64)
	if ok {
		C.rocksdb_options_increase_parallelism(o, C.int(tt))
	}

	ofpl, ok := config["optimize_for_point_lookup"].(float64)
	if ok {
		C.rocksdb_options_optimize_for_point_lookup(o, C.uint64_t(ofpl))
	}

	olsc, ok := config["optimize_level_style_compaction"].(float64)
	if ok {
		C.rocksdb_options_optimize_level_style_compaction(o, C.uint64_t(olsc))
	}

	ousc, ok := config["optimize_universal_style_compaction"].(float64)
	if ok {
		C.rocksdb_options_optimize_universal_style_compaction(o, C.uint64_t(ousc))
	}

	wbs, ok := config["write_buffer_size"].(float64)
	if ok {
		C.rocksdb_options_set_write_buffer_size(o, C.size_t(wbs))
	}

	mwbn, ok := config["max_write_buffer_number"].(float64)
	if ok {
		C.rocksdb_options_set_max_write_buffer_number(o, C.int(mwbn))
	}

	mwbntm, ok := config["min_write_buffer_number_to_merge"].(float64)
	if ok {
		C.rocksdb_options_set_min_write_buffer_number_to_merge(o, C.int(mwbntm))
	}

	mof, ok := config["max_open_files"].(float64)
	if ok {
		C.rocksdb_options_set_max_open_files(o, C.int(mof))
	}

	c, ok := config["compression"].(float64)
	if ok {
		C.rocksdb_options_set_compression(o, C.int(c))
	}

	mltc, ok := config["min_level_to_compress"].(float64)
	if ok {
		C.rocksdb_options_set_min_level_to_compress(o, C.int(mltc))
	}

	nl, ok := config["num_levels"].(float64)
	if ok {
		C.rocksdb_options_set_num_levels(o, C.int(nl))
	}

	lfnct, ok := config["level0_file_num_compaction_trigger"].(float64)
	if ok {
		C.rocksdb_options_set_level0_file_num_compaction_trigger(o, C.int(lfnct))
	}

	lswt, ok := config["level0_slowdown_writes_trigger"].(float64)
	if ok {
		C.rocksdb_options_set_level0_slowdown_writes_trigger(o, C.int(lswt))
	}

	lstopwt, ok := config["level0_stop_writes_trigger"].(float64)
	if ok {
		C.rocksdb_options_set_level0_stop_writes_trigger(o, C.int(lstopwt))
	}

	mmcl, ok := config["max_mem_compaction_level"].(float64)
	if ok {
		C.rocksdb_options_set_max_mem_compaction_level(o, C.int(mmcl))
	}

	tfsb, ok := config["target_file_size_base"].(float64)
	if ok {
		C.rocksdb_options_set_target_file_size_base(o, C.uint64_t(tfsb))
	}

	tfsm, ok := config["target_file_size_multiplier"].(float64)
	if ok {
		C.rocksdb_options_set_target_file_size_multiplier(o, C.int(tfsm))
	}

	mbflb, ok := config["max_bytes_for_level_base"].(float64)
	if ok {
		C.rocksdb_options_set_max_bytes_for_level_base(o, C.uint64_t(mbflb))
	}

	mbflm, ok := config["max_bytes_for_level_multiplier"].(float64)
	if ok {
		C.rocksdb_options_set_max_bytes_for_level_multiplier(o, C.int(mbflm))
	}

	ecf, ok := config["expanded_compaction_factor"].(float64)
	if ok {
		C.rocksdb_options_set_expanded_compaction_factor(o, C.int(ecf))
	}

	scf, ok := config["source_compaction_factor"].(float64)
	if ok {
		C.rocksdb_options_set_source_compaction_factor(o, C.int(scf))
	}

	mgof, ok := config["max_grandparent_overlap_factor"].(float64)
	if ok {
		C.rocksdb_options_set_max_grandparent_overlap_factor(o, C.int(mgof))
	}

	dds, ok := config["disable_data_sync"].(bool)
	if ok {
		C.rocksdb_options_set_disable_data_sync(o, C.int(btoi(dds)))
	}

	uf, ok := config["use_fsync"].(bool)
	if ok {
		C.rocksdb_options_set_use_fsync(o, C.int(btoi(uf)))
	}

	dofpm, ok := config["delete_obsolete_files_period_micros"].(float64)
	if ok {
		C.rocksdb_options_set_delete_obsolete_files_period_micros(o, C.uint64_t(dofpm))
	}

	mbc, ok := config["max_background_compactions"].(float64)
	if ok {
		C.rocksdb_options_set_max_background_compactions(o, C.int(mbc))
	}

	mbf, ok := config["max_background_flushes"].(float64)
	if ok {
		C.rocksdb_options_set_max_background_flushes(o, C.int(mbf))
	}

	mlfs, ok := config["max_log_file_size"].(float64)
	if ok {
		C.rocksdb_options_set_max_log_file_size(o, C.size_t(mlfs))
	}

	lfttr, ok := config["log_file_time_to_roll"].(float64)
	if ok {
		C.rocksdb_options_set_log_file_time_to_roll(o, C.size_t(lfttr))
	}

	klfn, ok := config["keep_log_file_num"].(float64)
	if ok {
		C.rocksdb_options_set_keep_log_file_num(o, C.size_t(klfn))
	}

	hrl, ok := config["hard_rate_limit"].(float64)
	if ok {
		C.rocksdb_options_set_hard_rate_limit(o, C.double(hrl))
	}

	rldmm, ok := config["rate_limit_delay_max_millisecond"].(float64)
	if ok {
		C.rocksdb_options_set_rate_limit_delay_max_milliseconds(o, C.uint(rldmm))
	}

	mmfs, ok := config["max_manifest_file_size"].(float64)
	if ok {
		C.rocksdb_options_set_max_manifest_file_size(o, C.size_t(mmfs))
	}

	tcnsb, ok := config["table_cache_numshardbits"].(float64)
	if ok {
		C.rocksdb_options_set_table_cache_numshardbits(o, C.int(tcnsb))
	}

	tcrscl, ok := config["table_cache_remove_scan_count_limit"].(float64)
	if ok {
		C.rocksdb_options_set_table_cache_remove_scan_count_limit(o, C.int(tcrscl))
	}

	abs, ok := config["arena_block_size"].(float64)
	if ok {
		C.rocksdb_options_set_arena_block_size(o, C.size_t(abs))
	}

	dac, ok := config["disable_auto_compactions"].(bool)
	if ok {
		C.rocksdb_options_set_disable_auto_compactions(o, C.int(btoi(dac)))
	}

	wts, ok := config["WAL_ttl_seconds"].(float64)
	if ok {
		C.rocksdb_options_set_WAL_ttl_seconds(o, C.uint64_t(wts))
	}

	wslm, ok := config["WAL_size_limit_MB"].(float64)
	if ok {
		C.rocksdb_options_set_WAL_size_limit_MB(o, C.uint64_t(wslm))
	}

	mps, ok := config["manifest_preallocation_size"].(float64)
	if ok {
		C.rocksdb_options_set_manifest_preallocation_size(o, C.size_t(mps))
	}

	prkwf, ok := config["purge_redundant_kvs_while_flush"].(bool)
	if ok {
		C.rocksdb_options_set_purge_redundant_kvs_while_flush(o, boolToChar(prkwf))
	}

	aob, ok := config["allow_os_buffer"].(bool)
	if ok {
		C.rocksdb_options_set_allow_os_buffer(o, boolToChar(aob))
	}

	amr, ok := config["allow_mmap_reads"].(bool)
	if ok {
		C.rocksdb_options_set_allow_mmap_reads(o, boolToChar(amr))
	}

	amw, ok := config["allow_mmap_writes"].(bool)
	if ok {
		C.rocksdb_options_set_allow_mmap_writes(o, boolToChar(amw))
	}

	sleor, ok := config["skip_log_error_on_recovery"].(bool)
	if ok {
		C.rocksdb_options_set_skip_log_error_on_recovery(o, boolToChar(sleor))
	}

	sdps, ok := config["stats_dump_period_sec"].(float64)
	if ok {
		C.rocksdb_options_set_stats_dump_period_sec(o, C.uint(sdps))
	}

	aroo, ok := config["advise_random_on_open"].(bool)
	if ok {
		C.rocksdb_options_set_advise_random_on_open(o, boolToChar(aroo))
	}

	ahocs, ok := config["access_hint_on_compaction_start"].(float64)
	if ok {
		C.rocksdb_options_set_access_hint_on_compaction_start(o, C.int(ahocs))
	}

	uam, ok := config["use_adaptive_mutex"].(bool)
	if ok {
		C.rocksdb_options_set_use_adaptive_mutex(o, boolToChar(uam))
	}

	bps, ok := config["bytes_per_sync"].(float64)
	if ok {
		C.rocksdb_options_set_bytes_per_sync(o, C.uint64_t(bps))
	}

	cs, ok := config["compaction_style"].(float64)
	if ok {
		C.rocksdb_options_set_compaction_style(o, C.int(cs))
	}

	vcic, ok := config["verify_checksums_in_compaction"].(bool)
	if ok {
		C.rocksdb_options_set_verify_checksums_in_compaction(o, boolToChar(vcic))
	}

	fd, ok := config["filter_deletes"].(bool)
	if ok {
		C.rocksdb_options_set_filter_deletes(o, boolToChar(fd))
	}

	mssii, ok := config["max_sequential_skip_in_iterations"].(float64)
	if ok {
		C.rocksdb_options_set_max_sequential_skip_in_iterations(o, C.uint64_t(mssii))
	}

	ius, ok := config["inplace_update_support"].(bool)
	if ok {
		C.rocksdb_options_set_inplace_update_support(o, boolToChar(ius))
	}

	iunl, ok := config["inplace_update_num_locks"].(float64)
	if ok {
		C.rocksdb_options_set_inplace_update_num_locks(o, C.size_t(iunl))
	}

	es, ok := config["enable_statistics"].(bool)
	if ok && es {
		C.rocksdb_options_enable_statistics(o)
	}

	pfbl, ok := config["prepare_for_bulk_load"].(bool)
	if ok && pfbl {
		C.rocksdb_options_prepare_for_bulk_load(o)
	}

	// options in the block based table options object
	bbto := C.rocksdb_block_based_options_create()

	lcc, ok := config["lru_cache_capacity"].(float64)
	if ok {
		cache := C.rocksdb_cache_create_lru(C.size_t(lcc))
		C.rocksdb_block_based_options_set_block_cache(bbto, cache)
	}

	bfbpk, ok := config["bloom_filter_bits_per_key"].(float64)
	if ok {
		bf := C.rocksdb_filterpolicy_create_bloom(C.int(bfbpk))
		C.rocksdb_block_based_options_set_filter_policy(bbto, bf)
	}

	// set the block based table options
	C.rocksdb_options_set_block_based_table_factory(o, bbto)

	return o, nil
}

func (s *Store) newWriteOptions() *C.rocksdb_writeoptions_t {
	wo := C.rocksdb_writeoptions_create()

	if s.woptSyncUse {
		C.rocksdb_writeoptions_set_sync(wo, boolToChar(s.woptSync))
	} else {
		// request fsync on write for safety by default
		C.rocksdb_writeoptions_set_sync(wo, boolToChar(true))
	}
	if s.woptDisableWALUse {
		C.rocksdb_writeoptions_disable_WAL(wo, C.int(btoi(s.woptDisableWAL)))
	}

	return wo
}

func (s *Store) newReadOptions() *C.rocksdb_readoptions_t {
	ro := C.rocksdb_readoptions_create()

	if s.roptVerifyChecksumsUse {
		C.rocksdb_readoptions_set_verify_checksums(ro, boolToChar(s.roptVerifyChecksums))
	}
	if s.roptFillCacheUse {
		C.rocksdb_readoptions_set_fill_cache(ro, boolToChar(s.roptFillCache))
	}
	if s.roptReadTierUse {
		C.rocksdb_readoptions_set_read_tier(ro, C.int(s.roptReadTier))
	}

	return ro
}
