/*-------------------------------------------------------------------------
 *
 * datalake_proxy.c
 *		datalake_agent Access to the big data agent
 *
 *
 *	Copyright Hashdata Development Group
 *
 *	IDENTIFICATION
 *		gpcontrib/datalake_proxy/datalake_proxy.c
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

/*
 * Exit status the JVM uses for -XX:+ExitOnOutOfMemoryError (HotSpot calls
 * os::exit(3) from report_java_out_of_memory).  Worth naming: that path writes
 * no hs_err file, dumps no core and runs no shutdown hook, so an OOM death is
 * indistinguishable from an external SIGKILL unless we decode the status.
 */
#define DLAGENT_EXIT_OOM 3

/* Module load */
void _PG_init(void);
void datalake_proxy_main(Datum main_arg);
static void datalake_proxy_start_worker(void);

/* Helper function */
static void startProxyProcess(pid_t pid);
static void DataLakeProxyLoop(pid_t pid);
static void DataLakeQuickdie(SIGNAL_ARGS);
static void reportProxyExit(int status);

bool IsUnderMasterDispatchMode(void);

/* GUC variables */
static bool register_datalake_proxy = true;

/* variables */
static volatile sig_atomic_t gotSIG = false;

int dlagent_memory_limit;

static void
DataLakeQuickdie(SIGNAL_ARGS)
{
	gotSIG = true;

	if(MyProc)
		SetLatch(&MyProc->procLatch);
}

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	/* can't define PGC_POSTMASTER variable after startup */
	DefineCustomBoolVariable("datalake_proxy.register_datalake_proxy",
							 "Starts the datalake proxy worker.",
							 NULL,
							 &register_datalake_proxy,
							 true,
							 PGC_POSTMASTER,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("datalake_proxy.dlagent_memory_limit",
							 "Sets the maximum memory to be used for dlagent guc unit mb.",
							 "Sets dlagent memory limit.",
							 &dlagent_memory_limit,
							 2048,
							 512, MAX_KILOBYTES / 1024,
							 PGC_SIGHUP,
							 GUC_UNIT_MB,
							 NULL,
							 NULL,
							 NULL);


	EmitWarningsOnPlaceholders("datalake_proxy");

	if (register_datalake_proxy)
		datalake_proxy_start_worker();
}

bool
IsUnderMasterDispatchMode(void)
{
	if (Gp_role == GP_ROLE_DISPATCH)
		return true;

	return false;
}

static void
datalake_proxy_start_worker(void)
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
	/*
	 * Restart with a 30s delay after a crash.  bgw_restart_time = 0 (the
	 * MemSet default) means "respawn immediately", which during transient
	 * failures (e.g. ENOSPC on relcache init file write) creates a tight
	 * 6-second cascade loop that floods the log and never lets the child
	 * dlagent JVM finish Spring startup.
	 */
	worker.bgw_restart_time = 30;

	strcpy(worker.bgw_library_name, "datalake_proxy");
	strcpy(worker.bgw_function_name, "datalake_proxy_main");
	strcpy(worker.bgw_name, "datalake proxy process");
	strcpy(worker.bgw_type, "datalake proxy process");

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
datalake_proxy_main(Datum main_arg)
{
	pid_t proxyPid;
	pid_t pid = getpid();

	pqsignal(SIGTERM, DataLakeQuickdie);
	pqsignal(SIGQUIT, DataLakeQuickdie);

	/* hashdata 3x default block sigquit */
	sigdelset(&BlockSig, SIGQUIT);

	switch (proxyPid = fork_process())
	{
		case -1:
			ereport(ERROR,
			        (errmsg("could not fork datalake proxy process: %m")));
			break;
		case 0:
			startProxyProcess(pid);

			/* if we're here an error occurred */
			exit(EXIT_FAILURE);
	}

	DataLakeProxyLoop(proxyPid);

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

	snprintf(maxMemoryLimit, sizeof(maxMemoryLimit), "-Xmx%dm", dlagent_memory_limit);
	proxyArgs[i++] = maxMemoryLimit;

	proxyArgs[i++] = "-XX:+ExitOnOutOfMemoryError";

	proxyArgs[i++] = "-jar";
	snprintf(jarFile, sizeof(jarFile), "%s/java/dlagent-1.0.0.jar", pkglib_path);
	proxyArgs[i++] = jarFile;

	snprintf(parentPid, sizeof(parentPid), "--parent.pid=%d", pid);
	proxyArgs[i++] = parentPid;

#ifndef USE_GOPHER
	/*
	 * Open-source build (configure --without-gopher): no libgopher is
	 * linked, so the agent must not select GopherFileIO -- it would try to
	 * connect to a gopher socket that no gophermeta process provides and
	 * fail every catalog metadata operation.  Turn gopher off on the agent
	 * side; it then uses the S3/HDFS FileIO driven by the volume connection
	 * info.  This overrides the gopher.enabled=true default packaged in the
	 * agent's application.properties.
	 */
	proxyArgs[i++] = "--gopher.enabled=false";
#endif

	proxyArgs[i] = NULL;

	execvp("java", proxyArgs);

	ereport(ERROR, (errmsg("could not start proxy process: %m")));
}

/*
 * Say why the dlagent child died.
 *
 * Without this the only trace of an out-of-memory death is the JVM's own
 * "Terminating due to java.lang.OutOfMemoryError" on stdout, which the syslogger
 * files under "3rd party error log" -- not in the agent's log, where anyone
 * investigating looks first.  Combined with the absent hs_err/core (see
 * DLAGENT_EXIT_OOM) that made issue #407 look like an external kill.
 */
static void
reportProxyExit(int status)
{
	if (WIFEXITED(status))
	{
		int			code = WEXITSTATUS(status);

		if (code == DLAGENT_EXIT_OOM)
			ereport(LOG,
					(errmsg("dlagent exited because the Java heap was exhausted"),
					 errdetail("The agent JVM runs with -XX:+ExitOnOutOfMemoryError and exits with status %d on the first OutOfMemoryError, leaving no stack trace, hs_err file or core dump.",
							   DLAGENT_EXIT_OOM),
					 errhint("Raise datalake_proxy.dlagent_memory_limit (currently %d MB).",
							 dlagent_memory_limit)));
		else
			ereport(LOG,
					(errmsg("dlagent exited with status %d", code)));
	}
	else if (WIFSIGNALED(status))
		ereport(LOG,
				(errmsg("dlagent was terminated by signal %d", WTERMSIG(status))));
}

static void
DataLakeProxyLoop(pid_t pid)
{
	int rc;

	while (true)
	{
		pid_t		reaped;
		int			status = 0;

		if (gotSIG)
		{
			gotSIG = false;
			kill(pid, SIGTERM);
			proc_exit(1);
		}

		reaped = waitpid(pid, &status, WNOHANG);
		if (reaped != 0)
		{
			/* status is only meaningful when we actually reaped the child */
			if (reaped > 0)
				reportProxyExit(status);
			proc_exit(1);
		}

		rc = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   1 * 1000L, WAIT_EVENT_BGWORKER_STARTUP);

		ResetLatch(&MyProc->procLatch);

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
		{
			kill(pid, SIGTERM);
			proc_exit(1);
		}
	}
}