/*-------------------------------------------------------------------------
 *
 * mux_server_client.h
 *	Multiplexed echo server/client with benchmarking (TCP/UDP, epoll/poll)
 *
 * Portions Copyright (c) 2025
 *
 * IDENTIFICATION
 *	mux_server_client.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef MUX_SERVER_CLIENT_H
#define MUX_SERVER_CLIENT_H

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <time.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <sys/time.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#ifdef __linux__
#include <sys/epoll.h>
#endif

#include <poll.h>

#ifdef __cplusplus
#include <atomic>
#define ATOMIC_BOOL              std::atomic<bool>
#define ATOMIC_INT		 std::atomic<uintptr_t>
#define ATOMIC_INIT_FALSE        false
#define ATOMIC_CAS(lock, expected, desired) \
	((lock)->compare_exchange_strong(expected, desired, \
		std::memory_order_acquire, std::memory_order_relaxed))
#define ATOMIC_STORE(lock, val)  \
	((lock)->store(val, std::memory_order_release))
#define ATOMIC_LOAD(lock)        \
	((lock)->load(std::memory_order_acquire))
#else
#include <stdatomic.h>
#define ATOMIC_BOOL              atomic_bool
#define ATOMIC_INT		 atomic_uintptr_t
#define ATOMIC_INIT_FALSE        ATOMIC_VAR_INIT(false)
#define ATOMIC_CAS(lock, expected, desired) \
	atomic_compare_exchange_strong_explicit(lock, &(expected), desired, \
		memory_order_acquire, memory_order_relaxed)
#define ATOMIC_STORE(lock, val)  \
	atomic_store_explicit(lock, val, memory_order_release)
#define ATOMIC_LOAD(lock)        \
	atomic_load_explicit(lock, memory_order_acquire)
#endif

#define CACHELINE_ALIGNED __attribute__((aligned(64)))

#if defined(__cplusplus) && defined(ATOMICLOCK)
#include <cstdint>

extern thread_local std::atomic<uintptr_t> g_tls_owner;
#endif
typedef struct CACHELINE_ALIGNED spinlock_t
{
	ATOMIC_BOOL lock;
	ATOMIC_INT owner; 
} spinlock_t;


/* Event type definitions */
#define MUX_EVENT_NONE	0x0
#define MUX_EVENT_READ	0x1
#define MUX_EVENT_WRITE	0x2

/* Callback return values */
#define MUX_CB_OK	0
#define MUX_CB_REMOVE	-1

/* Multiplexing backend types */
typedef enum
{
	MUX_BACKEND_POLL = 0,
	MUX_BACKEND_EPOLL = 1,
} MuxBackend;

/* Forward declaration for Mux */
typedef struct Mux Mux;

/* Event handler structure */
typedef struct MuxHandler
{
	void	*userdata;
	int	events;/* Interested events mask (MUX_EVENT_*) */
	int	(*on_read)(void *mux, int fd, void *ud);
	int	(*on_write)(void *mux, int fd, void *ud);
	int	(*on_error)(void *mux, int fd, void *ud);
	int	(*on_close)(void *mux, int fd, void *ud);
} MuxHandler;

/* Core multiplexer structure */
struct Mux
{
	MuxBackend	backend;
	ATOMIC_INT	stop_flag;

#ifdef __linux__
	int	epfd;
	struct HandlerEntry	**entries;
	size_t	entries_capacity;
	size_t	entries_count;
#endif

	/* Poll backend fields */
	struct pollfd	*pfds;
	MuxHandler	**handlers;
	size_t	pfds_capacity;
	size_t	pfds_count;
 	ATOMIC_BOOL init_ready;	
};

/* Internal handler entry for epoll backend (opaque to external) */
typedef struct HandlerEntry {
	int	fd;
	MuxHandler	*h;
} HandlerEntry;

/* Helper functions */
static inline int max_int(int a, int b) { return a > b ? a : b; }

/* Multiplexer core APIs */
Mux	*mux_create(MuxBackend backend);
void	mux_destroy(Mux *m);
int	mux_stop(Mux *m);
int	mux_add(Mux *m, int fd, int events, MuxHandler *handler);
int	mux_mod(Mux *m, int fd, int events);
int	mux_del(Mux *m, int fd);
int	mux_run(Mux *m, int timeout_ms);

/* Server/Client/Benchmark APIs */
extern int	run_echo_server(int port, MuxBackend backend);
extern int	run_client(const char *host, int port, const char *msg);
extern int	run_udp_server(int port, MuxBackend backend);
extern int	run_udp_client(const char *host, int port, const char *msg);
extern int	run_bench(const char *host, int port, int clients, int requests, const char *message);
extern int	run_udp_bench(const char *host, int port, int clients, int requests, const char *message);
extern int	run_nbtest(void);

inline void spinlock_init(spinlock_t *lock)
{
#if defined(__cplusplus) && defined(ATOMICLOCK)
	lock->lock.store(false, std::memory_order_relaxed);
	g_tls_owner.store(0, std::memory_order_relaxed);
#else
	atomic_init(&lock->lock, false);
	atomic_init(&lock->owner, 0);
#endif
}

inline bool spinlock_try_lock(spinlock_t *lock) 
{
 	bool expected = false;
#if defined(__cplusplus) && defined(ATOMICLOCK)
	if (ATOMIC_LOAD(&g_tls_owner))
                return true;
#else
	uint32_t self = (uint32_t)pthread_self();

	if (ATOMIC_LOAD(&lock->owner) == self)
        	return true;
#endif
	return ATOMIC_CAS(&lock->lock, expected, true);
}

inline void cpu_relax(void)
{
#if defined(__x86_64__) || defined(__i386__)
    __builtin_ia32_pause();
#elif defined(__aarch64__)
    __asm__ __volatile__("yield" ::: "memory");
#else
    __asm__ __volatile__("" ::: "memory");
#endif
}

inline void spinlock_lock(spinlock_t *lock) 
{
	if (spinlock_try_lock(lock))
 		return;

	unsigned int backoff = 1;
	unsigned int attempt = 0;
	const unsigned int max_backoff = 256;

	while (1) 
	{
		if (spinlock_try_lock(lock))
		{
#if defined(__cplusplus) && defined(ATOMICLOCK)
			g_tls_owner.store(1, std::memory_order_relaxed);
#else
			ATOMIC_STORE(&lock->owner, 1);
#endif
			return;
		}

		for (unsigned int i = 0; i < backoff; i++)
			cpu_relax();

		if (backoff < max_backoff)
			backoff = (backoff << 1) + (rand() % 4);

		if (++attempt % 128 == 0)
			sched_yield();
	}
}

inline void spinlock_unlock(spinlock_t *lock) 
{
	ATOMIC_STORE(&lock->lock, false);
	ATOMIC_STORE(&lock->owner, 0);
#if defined(__cplusplus) && defined(ATOMICLOCK)
	g_tls_owner.store(0, std::memory_order_relaxed);		
#endif
}
#endif /* MUX_SERVER_CLIENT_H */
