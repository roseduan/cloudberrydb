/*-------------------------------------------------------------------------
 *
 * hive_auto_sync.c
 *		hive_auto_sync Auto sync hive metadata
 *
 *
 *	Copyright cbdb Development Group
 *
 *	IDENTIFICATION
 *		contrib/datalake_apiary/hive_sync_bgworker/hive_auto_sync.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <signal.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <dirent.h>
#include <gopher/gopher_server.h>

#include "access/relation.h"
#include "access/xact.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "cdb/cdbvars.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/fork_process.h"
#include "postmaster/interrupt.h"
#include "libpq/pqsignal.h"
#include "storage/buf_internals.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/pmsignal.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/acl.h"
#include "utils/datetime.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relfilenodemap.h"
#include "utils/resowner.h"
#include "postmaster/postmaster.h"

PG_MODULE_MAGIC;


/* Module load */
void _PG_init(void);
PGDLLEXPORT void hive_sync_main(Datum main_arg);
static void hive_sync_start_worker(void);

/* Helper function */
static void startProxyProcess(pid_t pid);
static void hiveSyncMainLoop(void);
static void hiveSyncQuickdie(SIGNAL_ARGS);
static void hiveSyncReloadConfig(SIGNAL_ARGS);
static pid_t startHiveSync(void);

bool IsUnderMasterDispatchMode(void);

/* GUC variables */
static bool register_hive_synchronize = true;
static bool enable_hive_metastore_sync = false;
static char *custom_log4j_conf = false;
static int hivesync_memory_limit = 2048;

/* variables */
static volatile sig_atomic_t gotSIG = false;
static volatile sig_atomic_t gotSIGHUP = false;

static void
hiveSyncQuickdie(SIGNAL_ARGS)
{
	gotSIG = true;

	if(MyProc)
		SetLatch(&MyProc->procLatch);
}

static void
hiveSyncReloadConfig(SIGNAL_ARGS)
{
	gotSIGHUP = true;

	if (MyProc)
		SetLatch(&MyProc->procLatch);
}

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	/* can't define PGC_POSTMASTER variable after startup */
	DefineCustomBoolVariable("hive_sync.register_hive_sync",
							 "Registers the hive auto sync worker.",
							 NULL,
							 &register_hive_synchronize,
							 true,
							 PGC_POSTMASTER,
							 0,
							 NULL,
							 NULL,
							 NULL);
	
	DefineCustomBoolVariable("hive_sync.enable_hive_metastore_sync",
							 "Starts the hive auto sync worker.",
							 NULL,
							 &enable_hive_metastore_sync,
							 false,
							 PGC_SIGHUP,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("hive_sync.memory_limit",
							 "Sets the maximum memory to be used for hive auto sync guc unit mb.",
							 "Sets hive auto sync memory limit.",
							 &hivesync_memory_limit,
							 2048,
							 512, MAX_KILOBYTES / 1024,
							 PGC_SIGHUP,
							 GUC_UNIT_MB,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomStringVariable("hive_sync.custom_log4j_conf",
							   "Customize log4j configuration in root path.",
							   NULL,
							   &custom_log4j_conf,
							   false,
							   PGC_SIGHUP,
							   0,
							   NULL,
							   NULL,
							   NULL);

	EmitWarningsOnPlaceholders("hive_auto_sync");

	if (register_hive_synchronize)
		hive_sync_start_worker();
}

bool
IsUnderMasterDispatchMode(void)
{
	if (IS_QUERY_DISPATCHER())
		return true;

	return false;
}

static void
hive_sync_start_worker(void)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status;
	pid_t		pid;

	if (!IsUnderMasterDispatchMode())
	{
		return;
	}

	MemSet(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = 5;

	strcpy(worker.bgw_library_name, "hive_auto_sync");
	strcpy(worker.bgw_function_name, "hive_sync_main");
	strcpy(worker.bgw_name, "hive auto sync process");
	strcpy(worker.bgw_type, "hive auto sync process");

	if (process_shared_preload_libraries_in_progress)
	{
		RegisterBackgroundWorker(&worker);
		return;
	}
	/* must set notify PID to wait for startup */
	worker.bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not register background process"),
				 errhint("You may need to increase max_worker_processes.")));

	status = WaitForBackgroundWorkerStartup(handle, &pid);
	if (status != BGWH_STARTED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not start background process"),
				 errhint("More details may be available in the server log.")));
}

void
hive_sync_main(Datum main_arg)
{
	pqsignal(SIGTERM, hiveSyncQuickdie);
	pqsignal(SIGQUIT, hiveSyncQuickdie);
	pqsignal(SIGHUP, hiveSyncReloadConfig);

	/* cbdb 3x default block sigquit */
	sigdelset(&BlockSig, SIGQUIT);

	BackgroundWorkerUnblockSignals();

	hiveSyncMainLoop();

	proc_exit(0);
}

static void
startProxyProcess(pid_t pid)
{
	int   i = 0;
	char *proxyArgs[10];
	char  parentPid[128];
	char  maxMemoryLimit[128];
	char  jarFile[MAXPGPATH];

	proxyArgs[i++] = "java";
	proxyArgs[i++] = "-Xms512m";

	snprintf(maxMemoryLimit, sizeof(maxMemoryLimit), "-Xmx%dm", hivesync_memory_limit);
	proxyArgs[i++] = maxMemoryLimit;

	proxyArgs[i++] = "-XX:+ExitOnOutOfMemoryError";

	if (custom_log4j_conf)
	{
		proxyArgs[i++] = "-Dlog4j.configurationFile=log4j2-hivesync.xml";
	}

	proxyArgs[i++] = "-jar";
	snprintf(jarFile, sizeof(jarFile), "%s/java/hivesync-1.0.0.jar", pkglib_path);
	proxyArgs[i++] = jarFile;

	snprintf(parentPid, sizeof(parentPid), "--parent.pid=%d", pid);
	proxyArgs[i++] = parentPid;

	proxyArgs[i] = NULL;

	execvp("java", proxyArgs);

	ereport(ERROR, (errmsg("could not start proxy process: %m")));
}

static pid_t
startHiveSync()
{
	pid_t proxyPid;
	pid_t pid = getpid();
	switch (proxyPid = fork_process())
	{
		case -1:
			ereport(ERROR,
			        (errmsg("could not fork hive auto sync process: %m")));
			break;
		case 0:
			startProxyProcess(pid);

			/* if we're here an error occurred */
			exit(EXIT_FAILURE);
	}
	return proxyPid;
}

static void
hiveSyncMainLoop()
{
	int rc;
	pid_t pid = 0;
	while (true)
	{
		if (gotSIG)
		{
			gotSIG = false;
			if (pid > 0)
			{
				kill(pid, SIGTERM);
				pid = 0;
			}
			proc_exit(1);
		}

		if (gotSIGHUP)
		{
			gotSIGHUP = false;
			ereport(LOG,
					(errmsg("hive auto sync process received SIGHUP, restart to reload configuration.")));
			if (pid > 0)
			{
				kill(pid, SIGTERM);
				pid = 0;
			}
			ProcessConfigFile(PGC_SIGHUP);
		}

		if (enable_hive_metastore_sync && pid <= 0)
		{
			pid = startHiveSync();
			ereport(LOG,
					(errmsg("hive auto sync process started with pid %d", pid)));
		}

		if (pid > 0 && waitpid(pid, NULL, WNOHANG) != 0)
		{
			ereport(LOG,
					(errmsg("hive auto sync process with pid %d already dead, restarting", pid)));
			pid = 0;
			SetLatch(&MyProc->procLatch);
		}

		rc = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   1 * 1000L, WAIT_EVENT_BGWORKER_STARTUP);

		ResetLatch(&MyProc->procLatch);

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
		{
			if (pid > 0)
			{
				kill(pid, SIGTERM);
			}
			proc_exit(1);
		}
	}
}

