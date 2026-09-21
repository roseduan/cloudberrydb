#ifdef USE_GOPHERMETA
		"pg_gophermeta",
#endif
#ifdef USE_VECTORIZATION
		"vectorization",
#endif
/* dfs_tablespace should be loaded before pax_storage,
* dfs_tablespace should be loaded before other extensions that
* hook the ProcessUtility_hook function
*/
#ifdef USE_DFS_TABLESPACE
		"dfs_tablespace",
#endif
#ifdef ENABLE_PRELOAD_IC_MODULE
  "interconnect",
#endif
#ifdef ENABLE_IC_UDP2
  		"udp2",
#endif
#ifdef USE_PAX_STORAGE
		"pax",
#endif
#ifdef USE_PERFMON
		"gpmmon","gpmon",
#endif
#ifdef USE_DATALAKE
		"datalake_proxy",
		"hive_auto_sync",
		"datalake_fdw",
#endif

		"fixeddecimal",
