/*-------------------------------------------------------------------------
 *
 * ic_mux.c
 *
 * Portions Copyright (c) 2025
 *
 * IDENTIFICATION
 *
 *-------------------------------------------------------------------------
 */

#include "ic_mux.h"
#define MAX_EVENTS  64
/* Set/clear non-blocking mode for a file descriptor */
static int
set_nonblocking(int fd, int nonblock)
{
	int	flags = fcntl(fd, F_GETFL, 0);

	if (flags < 0)
		return -1;

	if (nonblock)
		flags |= O_NONBLOCK;
	else
		flags &= ~O_NONBLOCK;

	return fcntl(fd, F_SETFL, flags) < 0 ? -1 : 0;
}

/* Ensure poll arrays have enough capacity for n elements */
static int
ensure_poll_capacity(Mux *m, size_t n)
{
	if (m->pfds_capacity >= n)
		return 0;

	size_t	newcap = (m->pfds_capacity == 0) ? 16 : m->pfds_capacity * 2;
	while (newcap < n)
		newcap *= 2;

	struct pollfd *p = realloc(m->pfds, sizeof(struct pollfd) * newcap);
	if (!p)
		return -1;
	/*
	 * Commit the reallocated pfds buffer immediately. realloc() has already
	 * freed the previous buffer, so leaving m->pfds pointing at it would be a
	 * dangling pointer if the handlers realloc below fails.
	 */
	m->pfds = p;

	MuxHandler **h = realloc(m->handlers, sizeof(MuxHandler *) * newcap);
	if (!h)
		return -1;	/* m->pfds is already valid; capacity stays unchanged */

	m->handlers = h;
	m->pfds_capacity = newcap;

	return 0;
}

#ifdef __linux__
/* Ensure epoll entries array has enough capacity for n elements */
static int
ensure_entries_capacity(Mux *m, size_t n)
{
	if (m->entries_capacity >= n)
		return 0;

	size_t	newcap = (m->entries_capacity == 0) ? 16 : m->entries_capacity * 2;
	while (newcap < n)
		newcap *= 2;

	HandlerEntry **p = realloc(m->entries, sizeof(HandlerEntry *) * newcap);
	if (!p)
		return -1;

	m->entries = p;
	m->entries_capacity = newcap;

	return 0;
}

/* Find epoll handler entry by file descriptor */
static HandlerEntry *
find_entry_by_fd(Mux *m, int fd, size_t *out_idx)
{
	if (!m)
		return NULL;

	for (size_t i = 0; i < m->entries_count; ++i)
	{
		if (m->entries[i] && m->entries[i]->fd == fd)
		{
			if (out_idx)
				*out_idx = i;
			return m->entries[i];
		}
	}

	return NULL;
}
#endif

/* Create new multiplexer instance with specified backend */
Mux *
mux_create(MuxBackend backend)
{
	Mux	*m = calloc(1, sizeof(Mux));

	if (!m)
		return NULL;

	m->stop_flag = 0;
	m->init_ready = false;
#ifdef __linux__
	m->entries = NULL;
	m->entries_capacity = 0;
	m->entries_count = 0;

	if (backend == MUX_BACKEND_EPOLL)
	{
		m->epfd = epoll_create1(EPOLL_CLOEXEC);
		if (m->epfd < 0)
		{
#ifdef DEBUG
			perror("epoll_create1");
#endif
			free(m);
			return NULL;
		}
	}
	else
	{
		m->epfd = -1;
	}
#else
	if (backend == MUX_BACKEND_EPOLL)
	{
		fprintf(stderr, "epoll not supported, using poll\n");
		backend = MUX_BACKEND_POLL;
	}
#endif

	m->backend = backend;
	m->pfds = NULL;
	m->handlers = NULL;
	m->pfds_capacity = 0;
	m->pfds_count = 0;

	return m;
}

/* Destroy multiplexer and release all resources */
void
mux_destroy(Mux *m)
{
	if (!m)
		return;

#ifdef __linux__
	if (m->backend == MUX_BACKEND_EPOLL && m->epfd >= 0)
		close(m->epfd);

	if (m->entries)
	{
		for (size_t i = 0; i < m->entries_count; ++i)
		{
			HandlerEntry *e = m->entries[i];
			/* Same as the poll backend: on a stop_flag shutdown no close
			 * events are emitted, so release each handler's userdata/handler
			 * via on_close before freeing the entry. */
			if (e && e->h && e->h->on_close)
				e->h->on_close(m, e->fd, e->h->userdata);
			free(e);
		}
		free(m->entries);
	}
#endif

	/* On stop_flag shutdown mux_run emits no close events, so handlers still
	 * registered would leak their userdata/handler. Invoke on_close for each
	 * remaining entry before freeing the arrays (poll backend). */
	if (m->handlers)
	{
		for (size_t i = 0; i < m->pfds_count; ++i)
		{
			MuxHandler *h = m->handlers[i];
			if (h && h->on_close)
				h->on_close(m, m->pfds ? m->pfds[i].fd : -1, h->userdata);
		}
	}

	if (m->pfds)
		free(m->pfds);
	if (m->handlers)
		free(m->handlers);

	free(m);
}

/* Stop multiplexer event loop */
int
mux_stop(Mux *m)
{
	if (!m)
		return -1;

	m->stop_flag = 1;
	return 0;
}

/* Add file descriptor to multiplexer with event handlers */
int
mux_add(Mux *m, int fd, int events, MuxHandler *handler)
{
	if (!m || fd < 0 || !handler)
		return -1;

	handler->events = events;

#ifdef __linux__
	if (m->backend == MUX_BACKEND_EPOLL)
	{
		if (find_entry_by_fd(m, fd, NULL))
			return mux_mod(m, fd, events);

		HandlerEntry *entry = malloc(sizeof(HandlerEntry));
		if (!entry)
			return -1;

		entry->fd = fd;
		entry->h = handler;

		struct epoll_event ev;
		ev.events = 0;
		if (events & MUX_EVENT_READ)
			ev.events |= EPOLLIN | EPOLLRDHUP;
		if (events & MUX_EVENT_WRITE)
			ev.events |= EPOLLOUT;
		ev.data.ptr = entry;

		if (epoll_ctl(m->epfd, EPOLL_CTL_ADD, fd, &ev) < 0)
		{
#ifdef DEBUG
			perror("epoll_ctl ADD");
#endif
			free(entry);
			return -1;
		}

		if (ensure_entries_capacity(m, m->entries_count + 1) < 0)
		{
			epoll_ctl(m->epfd, EPOLL_CTL_DEL, fd, NULL);
			free(entry);
			return -1;
		}

		m->entries[m->entries_count++] = entry;
		return 0;
	}
#endif

	/* Poll backend implementation */
	size_t		idx = m->pfds_count;

	if (ensure_poll_capacity(m, idx + 1) < 0)
		return -1;

	m->pfds[idx].fd = fd;
	m->pfds[idx].events = 0;
	if (events & MUX_EVENT_READ)
		m->pfds[idx].events |= POLLIN;
	if (events & MUX_EVENT_WRITE)
		m->pfds[idx].events |= POLLOUT;
	m->pfds[idx].revents = 0;
	m->handlers[idx] = handler;
	m->pfds_count++;

	return 0;
}

/* Modify event mask for registered file descriptor */
int
mux_mod(Mux *m, int fd, int events)
{
	if (!m || fd < 0)
		return -1;

#ifdef __linux__
	if (m->backend == MUX_BACKEND_EPOLL)
	{
		size_t	idx = 0;
		HandlerEntry *entry = find_entry_by_fd(m, fd, &idx);

		if (!entry)
			return -1;

		struct epoll_event ev;
		ev.events = 0;
		if (events & MUX_EVENT_READ)
			ev.events |= EPOLLIN | EPOLLRDHUP;
		if (events & MUX_EVENT_WRITE)
			ev.events |= EPOLLOUT;
		ev.data.ptr = entry;

		if (epoll_ctl(m->epfd, EPOLL_CTL_MOD, fd, &ev) < 0)
		{
#ifdef DEBUG
			perror("epoll_ctl MOD");
#endif
			return -1;
		}

		entry->h->events = events;
		return 0;
	}
#endif

	/* Poll backend implementation */
	int found = 0;
	for (size_t i = 0; i < m->pfds_count; ++i)
	{
		if (m->pfds[i].fd == fd)
		{
			m->pfds[i].events = 0;
			if (events & MUX_EVENT_READ)
				m->pfds[i].events |= POLLIN;
			if (events & MUX_EVENT_WRITE)
				m->pfds[i].events |= POLLOUT;
			m->handlers[i]->events = events;
			found = 1;
		}
	}

	return found ? 0 : -1;
}

/* Remove file descriptor from multiplexer */
int
mux_del(Mux *m, int fd)
{
	if (!m || fd < 0)
		return -1;

#ifdef __linux__
	if (m->backend == MUX_BACKEND_EPOLL)
	{
		size_t	idx = 0;
		HandlerEntry *entry = find_entry_by_fd(m, fd, &idx);

		epoll_ctl(m->epfd, EPOLL_CTL_DEL, fd, NULL);

		if (!entry)
			return 0;

		free(entry);
		if (idx + 1 < m->entries_count)
		{
			memmove(&m->entries[idx], &m->entries[idx + 1],
					sizeof(HandlerEntry *) * (m->entries_count - idx - 1));
		}
		m->entries_count--;

		return 0;
	}
#endif

	/* Poll backend implementation */
	for (size_t i = 0; i < m->pfds_count; ++i)
	{
		if (m->pfds[i].fd == fd)
		{
			if (i + 1 < m->pfds_count)
			{
				memmove(&m->pfds[i], &m->pfds[i + 1],
						sizeof(struct pollfd) * (m->pfds_count - i - 1));
				memmove(&m->handlers[i], &m->handlers[i + 1],
						sizeof(MuxHandler *) * (m->pfds_count - i - 1));
			}
			m->pfds_count--;

			return 0;
		}
	}

	return 0;
}

/* Run multiplexer event loop */
int
mux_run(Mux *m, int timeout_ms)
{
	if (!m)
		return -1;

	/*
	 * Do NOT reset stop_flag here. The caller may release its lock just before
	 * invoking mux_run(), and TeardownUDP/SendEOS can set stop_flag in that
	 * window; clearing it would silently discard the stop request and the event
	 * loop would never exit. stop_flag is zero-initialized in mux_create().
	 */
	while (!m->stop_flag)
	{
#ifdef __linux__
		if (m->backend == MUX_BACKEND_EPOLL)
		{
			struct epoll_event events[MAX_EVENTS];
			int	n;

			do {
				n = epoll_wait(m->epfd, events, MAX_EVENTS, timeout_ms);
			} while (n < 0 && errno == EINTR);

			if (n < 0)
			{
#ifdef DEBUG
				perror("epoll_wait");
#endif
				return -1;
			}
			if (n == 0)
				continue;

			for (int i = 0; i < n; ++i)
			{
				HandlerEntry *entry = (HandlerEntry *) events[i].data.ptr;
				if (!entry)
					continue;

				MuxHandler *h = entry->h;
				int	fd = entry->fd;
				uint32_t	ev = events[i].events;
				int	remove = 0;

				if (ev & (EPOLLERR | EPOLLHUP | EPOLLRDHUP))
				{
					if (h && h->on_error)
						h->on_error(m, fd, h->userdata);
					mux_del(m, fd);
					if (h && h->on_close)
						h->on_close(m, fd, h->userdata);
					continue;
				}

				if ((ev & EPOLLIN) && h && h->on_read)
				{
					int	r = h->on_read(m, fd, h->userdata);
					if (r < 0)
						remove = 1;
				}

				if (!remove && (ev & EPOLLOUT) && h && h->on_write)
				{
					int	r = h->on_write(m, fd, h->userdata);
					if (r < 0)
						remove = 1;
				}

				if (remove)
				{
					mux_del(m, fd);
					if (h && h->on_close)
						h->on_close(m, fd, h->userdata);
				}
			}
			continue;
		}
#endif

		/* Poll backend implementation */
		int	rc;

		do {
			rc = poll(m->pfds, (nfds_t) m->pfds_count, timeout_ms);
		} while (rc < 0 && errno == EINTR);

		if (rc < 0)
		{
#ifdef DEBUG
			perror("poll");
#endif
			return -1;
		}
		if (rc == 0)
			continue;

		for (size_t i = 0; i < m->pfds_count; ++i)
		{
			short   ur = m->pfds[i].events;
			short	re = m->pfds[i].revents;
			if (!re)
				continue;

			m->pfds[i].revents = 0;

			MuxHandler *h = m->handlers[i];
			int	fd = m->pfds[i].fd;
			int	remove = 0;

			if (re & (POLLERR | POLLNVAL))
			{
				if (h && h->on_error)
					h->on_error(m, fd, h->userdata);
				mux_del(m, fd);
				if (h && h->on_close)
					h->on_close(m, fd, h->userdata);
				i--;
				continue;
			}

			if ((re & POLLHUP) && !(re & POLLIN))
			{
				if (h && h->on_close)
					h->on_close(m, fd, h->userdata);
				mux_del(m, fd);
				i--;
				continue;
			}

			if ((re & POLLIN) && h && h->on_read && (ur & POLLIN))
			{
				int	r = h->on_read(m, fd, h->userdata);
				if (r < 0)
					remove = 1;
			}

			if (!remove && (re & POLLOUT) && h && h->on_write && (ur & POLLOUT))
			{
				int	r = h->on_write(m, fd, h->userdata);
				if (r < 0)
					remove = 1;
			}

			if (remove)
			{
				if (h && h->on_close)
					h->on_close(m, fd, h->userdata);
				mux_del(m, fd);
				i--;
			}
		}
	}

	return 0;
}

/* ---------------- Echo Server Callbacks ---------------- */

/* Handle read events for TCP echo server clients (echo data back) */
static int
on_client_read(void *_mux, int fd, void *ud __attribute__((unused)))
{
	Mux	*mux = (Mux *) _mux;
	char	buf[8192];

	while (1)
	{
		ssize_t	n = recv(fd, buf, sizeof(buf), 0);

		if (n > 0)
		{
			ssize_t	sent = 0;
			while (sent < n)
			{
				ssize_t	w = send(fd, buf + sent, (size_t) (n - sent), 0);
				if (w >= 0)
				{
					sent += w;
					continue;
				}

				if (w < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
				{
					mux_mod(mux, fd, MUX_EVENT_READ | MUX_EVENT_WRITE);
					return MUX_CB_OK;
				}

				if (w < 0 && errno == EINTR)
					continue;
#ifdef DEBUG
				perror("send");
#endif
				return MUX_CB_REMOVE;
			}
		}
		else if (n == 0)
		{
			return MUX_CB_REMOVE;
		}
		else
		{
			if (errno == EAGAIN || errno == EWOULDBLOCK)
				break;
			else if (errno == EINTR)
				continue;
			else
			{
#ifdef DEBUG
				perror("recv");
#endif
				return MUX_CB_REMOVE;
			}
		}
	}

	return MUX_CB_OK;
}

/* Handle write events for TCP echo server clients (switch back to read) */
static int
on_client_write(void *_mux, int fd, void *ud __attribute__((unused)))
{
	Mux	*mux = (Mux *) _mux;

	mux_mod(mux, fd, MUX_EVENT_READ);
	return MUX_CB_OK;
}

/* Handle close events for TCP echo server clients (cleanup resources) */
static int
on_client_close(void *_mux __attribute__((unused)), int fd, void *ud)
{
	MuxHandler *handler = (MuxHandler *) ud;

	if (fd >= 0)
		close(fd);
	if (handler)
		free(handler);

	return MUX_CB_OK;
}

/* Handle error events for TCP echo server clients (dummy handler) */
static int
on_client_error(void *_mux __attribute__((unused)), int fd __attribute__((unused)), void *ud __attribute__((unused)))
{
	return MUX_CB_OK;
}

/* Handle read events for server socket (accept new connections) */
static int
on_server_read(void *_mux, int fd, void *ud __attribute__((unused)))
{
	Mux	*mux = (Mux *) _mux;

	while (1)
	{
		struct sockaddr_in cli;
		socklen_t	len = sizeof(cli);
		int	c = accept(fd, (struct sockaddr *) &cli, &len);

		if (c < 0)
		{
			if (errno == EAGAIN || errno == EWOULDBLOCK)
				break;
			if (errno == EINTR)
				continue;
#ifdef DEBUG
			perror("accept");
#endif
			return MUX_CB_OK;
		}

		set_nonblocking(c, 1);

		MuxHandler *ch = malloc(sizeof(MuxHandler));
		if (!ch)
		{
			close(c);
			continue;
		}

		memset(ch, 0, sizeof(*ch));
		ch->userdata = ch;
		ch->on_read = on_client_read;
		ch->on_write = on_client_write;
		ch->on_close = on_client_close;
		ch->on_error = on_client_error;

		if (mux_add(mux, c, MUX_EVENT_READ, ch) < 0)
		{
			close(c);
			free(ch);
		}
	}

	return MUX_CB_OK;
}

/* Handle close events for server socket (cleanup listening fd) */
static int
on_server_close(void *_mux __attribute__((unused)), int fd, void *ud __attribute__((unused)))
{
	close(fd);
	return MUX_CB_OK;
}

/* ---------------- TCP Echo Server Implementation ---------------- */

/* Run TCP echo server on specified port with selected backend */
int
run_echo_server(int port, MuxBackend backend)
{
	int	sfd = socket(AF_INET, SOCK_STREAM, 0);

	if (sfd < 0)
	{
#ifdef DEBUG
		perror("socket");
#endif
		return -1;
	}

	int	opt = 1;
	setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

	struct sockaddr_in addr;
	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = INADDR_ANY;
	addr.sin_port = htons((uint16_t) port);

	if (bind(sfd, (struct sockaddr *) &addr, sizeof(addr)) < 0)
	{
#ifdef DEBUG
		perror("bind");
#endif
		close(sfd);
		return -1;
	}

	if (listen(sfd, 128) < 0)
	{
#ifdef DEBUG
		perror("listen");
#endif
		close(sfd);
		return -1;
	}

	set_nonblocking(sfd, 1);

	Mux	*mux = mux_create(backend);
	if (!mux)
	{
		fprintf(stderr, "mux_create failed\n");
		close(sfd);
		return -1;
	}

	MuxHandler *sh = malloc(sizeof(MuxHandler));
	if (!sh)
	{
		close(sfd);
		mux_destroy(mux);
		return -1;
	}

	memset(sh, 0, sizeof(*sh));
	sh->userdata = sh;
	sh->on_read = on_server_read;
	sh->on_close = on_server_close;
	sh->on_error = on_client_error;

	if (mux_add(mux, sfd, MUX_EVENT_READ, sh) < 0)
	{
		fprintf(stderr, "mux_add server failed\n");
		close(sfd);
		free(sh);
		mux_destroy(mux);
		return -1;
	}

#ifdef DEBUG
	printf("Echo server listening on port %d (backend %s)\n", port,
		   backend == MUX_BACKEND_EPOLL ? "epoll" : "poll");
#endif

	mux_run(mux, 1000);

	mux_del(mux, sfd);
	free(sh);
	mux_destroy(mux);
	close(sfd);

	return 0;
}

/* ---------------- TCP Client Implementation ---------------- */

/* Run TCP echo client to send message to specified server */
int
run_client(const char *host, int port, const char *msg)
{
	int	c = socket(AF_INET, SOCK_STREAM, 0);

	if (c < 0)
	{
#ifdef DEBUG
		perror("socket");
#endif
		return -1;
	}

	struct sockaddr_in addr;
	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_port = htons((uint16_t) port);

	if (inet_pton(AF_INET, host, &addr.sin_addr) != 1)
	{
#ifdef DEBUG
		perror("inet_pton");
#endif
		close(c);
		return -1;
	}

	if (connect(c, (struct sockaddr *) &addr, sizeof(addr)) < 0)
	{
#ifdef DEBUG
		perror("connect");
#endif
		close(c);
		return -1;
	}

	ssize_t		s = send(c, msg, strlen(msg), 0);
	if (s < 0)
	{
#ifdef DEBUG
		perror("send");
#endif
		close(c);
		return -1;
	}

	char	buf[8192];
	ssize_t		n = recv(c, buf, sizeof(buf) - 1, 0);
	if (n < 0)
	{
#ifdef DEBUG
		perror("recv");
#endif
		close(c);
		return -1;
	}

	buf[n] = '\0';
#ifdef DEBUG
	printf("client received: %s\n", buf);
#endif
	close(c);

	return 0;
}

/* ---------------- UDP Server/Client Implementation ---------------- */

/* UDP server context structure */
typedef struct {
	struct sockaddr_in addr;
} UdpServerCtx;

/* Handle read events for UDP echo server (echo data back to client) */
static int
udp_on_read(void *_mux __attribute__((unused)), int fd, void *ud __attribute__((unused)))
{
	char	buf[8192];
	struct sockaddr_in cli;
	socklen_t	len = sizeof(cli);

	while (1)
	{
		ssize_t	n = recvfrom(fd, buf, sizeof(buf), 0, (struct sockaddr *) &cli, &len);

		if (n < 0)
		{
			if (errno == EAGAIN || errno == EWOULDBLOCK)
				return MUX_CB_OK;
			if (errno == EINTR)
				continue;

			perror("recvfrom");
			return -1;
		}
		if (n == 0)
			return MUX_CB_OK;
		sendto(fd, buf, (size_t) n, 0, (struct sockaddr *) &cli, len);
	}

	return MUX_CB_OK;
}

/* Run UDP echo server on specified port with selected backend */
int
run_udp_server(int port, MuxBackend backend)
{
	int	sfd = socket(AF_INET, SOCK_DGRAM, 0);

	if (sfd < 0)
	{
#ifdef DEBUG
		perror("socket");
#endif
		return -1;
	}

	int	opt = 1;
	setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

	struct sockaddr_in addr;
	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = INADDR_ANY;
	addr.sin_port = htons((uint16_t) port);

	if (bind(sfd, (struct sockaddr *) &addr, sizeof(addr)) < 0)
	{
#ifdef DEBUG
		perror("bind");
#endif
		close(sfd);
		return -1;
	}

	set_nonblocking(sfd, 1);

	Mux	*mux = mux_create(backend);
	if (!mux)
	{
#ifdef DEBUG
		fprintf(stderr, "mux_create failed\n");
#endif
		close(sfd);
		return -1;
	}

	MuxHandler *handler = calloc(1, sizeof(MuxHandler));
	UdpServerCtx *ctx = calloc(1, sizeof(UdpServerCtx));
	if (!handler || !ctx)
	{
		free(handler);
		free(ctx);
		mux_destroy(mux);
		close(sfd);
		return -1;
	}
	ctx->addr = addr;
	handler->userdata = ctx;
	handler->on_read = udp_on_read;

	if (mux_add(mux, sfd, MUX_EVENT_READ, handler) < 0)
	{
#ifdef DEBUG
		perror("mux_add");
#endif
		free(ctx);
		free(handler);
		mux_destroy(mux);
		close(sfd);
		return -1;
	}
#ifdef DEBUG
	printf("UDP echo server listening on port %d (%s)\n",
		   port, backend == MUX_BACKEND_EPOLL ? "epoll" : "poll");
#endif
	mux_run(mux, 1000);

	mux_destroy(mux);
	free(handler);
	free(ctx);
	close(sfd);

	return 0;
}

/* Run UDP echo client to send message to specified server */
int
run_udp_client(const char *host, int port, const char *msg)
{
	int	fd = socket(AF_INET, SOCK_DGRAM, 0);

	if (fd < 0)
	{
#ifdef DEBUG
		perror("socket");
#endif
		return -1;
	}

	struct sockaddr_in addr;
	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_port = htons(port);

	if (inet_pton(AF_INET, host, &addr.sin_addr) != 1)
	{
#ifdef DEBUG
		perror("inet_pton");
#endif
		close(fd);
		return -1;
	}

	ssize_t	n = sendto(fd, msg, strlen(msg), 0, (struct sockaddr *) &addr, sizeof(addr));
	if (n < 0)
	{
#ifdef DEBUG
		perror("sendto");
#endif
		close(fd);
		return -1;
	}

	char	buf[8192];
	struct sockaddr_in src;
	socklen_t	slen = sizeof(src);
	ssize_t	r = recvfrom(fd, buf, sizeof(buf) - 1, 0, (struct sockaddr *) &src, &slen);

	if (r < 0)
	{
#ifdef DEBUG
		perror("recvfrom");
#endif
		close(fd);
		return -1;
	}

	buf[r] = '\0';
#ifdef DEBUG
	printf("UDP client received: %s\n", buf);
#endif
	close(fd);

	return 0;
}

/* ---------------- Benchmark Related Implementations ---------------- */

/* Benchmark statistics structure */
typedef struct {
	int	total_clients;
	int	completed_clients;
	int	total_requests;
	int	completed_requests;
	int	failed_requests;
	double	total_latency;
	struct timespec bench_start;
	struct timespec bench_end;
	Mux	*mux;
} BenchStats;

/* Benchmark client context structure */
typedef struct {
	int	fd;
	int	state;	/* 0=connecting,1=ready,2=sending,3=receiving */
	const char *message;
	size_t	msg_len;
	size_t	sent;
	size_t	received;
	int	requests_planned;
	int	requests_completed;
	struct timespec start_time;
	struct timespec end_time;
	void	*userdata;
} BenchClient;

/* Start new request for benchmark client */
static void
start_request(Mux *mux, BenchClient *client)
{
	client->state = 2;
	client->sent = 0;
	client->received = 0;
	clock_gettime(CLOCK_MONOTONIC, &client->start_time);
	mux_mod(mux, client->fd, MUX_EVENT_WRITE);
}

/* Handle write events for benchmark clients (send requests) */
static int
bench_client_on_write(void *_mux, int fd, void *ud)
{
	Mux	*mux = (Mux *) _mux;
	BenchClient *client = (BenchClient *) ud;
	BenchStats *stats = (BenchStats *) client->userdata;

	if (client->state == 0)
	{
		int	error = 0;
		socklen_t	len = sizeof(error);

		if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &len) < 0 || error != 0)
		{
			stats->failed_requests += client->requests_planned;
			stats->completed_clients++;
			return MUX_CB_REMOVE;
		}

		client->state = 1;
		start_request(mux, client);
		return MUX_CB_OK;
	}

	if (client->state == 2)
	{
		while (client->sent < client->msg_len)
		{
			ssize_t	n = send(fd, client->message + client->sent, client->msg_len - client->sent, 0);

			if (n < 0)
			{
				if (errno == EAGAIN || errno == EWOULDBLOCK)
					return MUX_CB_OK;
				if (errno == EINTR)
					continue;

				stats->failed_requests += client->requests_planned - client->requests_completed;
				stats->completed_clients++;
				return MUX_CB_REMOVE;
			}

			client->sent += n;
		}

		client->state = 3;
		mux_mod(mux, fd, MUX_EVENT_READ);
		return MUX_CB_OK;
	}

	return MUX_CB_OK;
}

/* Handle read events for benchmark clients (receive responses) */
static int
bench_client_on_read(void *_mux, int fd, void *ud)
{
	Mux	*mux = (Mux *) _mux;
	BenchClient *client = (BenchClient *) ud;
	BenchStats *stats = (BenchStats *) client->userdata;

	if (client->state != 3)
		return MUX_CB_OK;

	char	buf[4096];
	while (client->received < client->msg_len)
	{
		ssize_t	n = recv(fd, buf, sizeof(buf), 0);

		if (n < 0)
		{
			if (errno == EAGAIN || errno == EWOULDBLOCK)
				return MUX_CB_OK;
			if (errno == EINTR)
				continue;

			stats->failed_requests += client->requests_planned - client->requests_completed;
			stats->completed_clients++;
			return MUX_CB_REMOVE;
		}

		if (n == 0)
		{
			stats->failed_requests += client->requests_planned - client->requests_completed;
			stats->completed_clients++;
			return MUX_CB_REMOVE;
		}

		client->received += n;
	}

	clock_gettime(CLOCK_MONOTONIC, &client->end_time);
	double	latency = (client->end_time.tv_sec - client->start_time.tv_sec) * 1000.0 +
		(client->end_time.tv_nsec - client->start_time.tv_nsec) / 1000000.0;

	stats->total_latency += latency;
	stats->completed_requests++;
	client->requests_completed++;

	if (client->requests_completed < client->requests_planned)
	{
		start_request(mux, client);
	}
	else
	{
		stats->completed_clients++;
		if (stats->completed_clients >= stats->total_clients)
			mux_stop(stats->mux);

		return MUX_CB_REMOVE;
	}

	return MUX_CB_OK;
}

/* Handle error events for benchmark clients (count failures) */
static int
bench_client_on_error(void *_mux __attribute__((unused)), int fd __attribute__((unused)), void *ud)
{
	BenchClient *client = (BenchClient *) ud;
	BenchStats *stats = (BenchStats *) client->userdata;

	stats->failed_requests += client->requests_planned - client->requests_completed;
	stats->completed_clients++;

	if (stats->completed_clients >= stats->total_clients)
		mux_stop(stats->mux);

	return MUX_CB_REMOVE;
}

/* Handle close events for benchmark clients (cleanup resources) */
static int
bench_client_on_close(void *_mux __attribute__((unused)), int fd, void *ud)
{
	BenchClient *client = (BenchClient *) ud;

	close(fd);
	free(client);
	return MUX_CB_OK;
}

/* Run TCP benchmark with specified clients/requests/message */
int
run_bench(const char *host, int port, int clients, int requests, const char *message)
{
	Mux	*mux = mux_create(MUX_BACKEND_EPOLL);

	if (!mux)
	{
		fprintf(stderr, "Failed to create mux\n");
		return -1;
	}

	BenchStats stats = {
		.total_clients = clients,
		.completed_clients = 0,
		.total_requests = clients * requests,
		.completed_requests = 0,
		.failed_requests = 0,
		.total_latency = 0.0,
		.mux = mux
	};
	clock_gettime(CLOCK_MONOTONIC, &stats.bench_start);

	for (int i = 0; i < clients; i++)
	{
		int	fd = socket(AF_INET, SOCK_STREAM, 0);
		if (fd < 0)
		{
#ifdef DEBUG
			perror("socket");
#endif
			continue;
		}

		set_nonblocking(fd, 1);

		struct sockaddr_in addr;
		memset(&addr, 0, sizeof(addr));
		addr.sin_family = AF_INET;
		addr.sin_port = htons(port);

		if (inet_pton(AF_INET, host, &addr.sin_addr) != 1)
		{
#ifdef DEBUG
			perror("inet_pton");
#endif
			close(fd);
			continue;
		}

		int	rc = connect(fd, (struct sockaddr *) &addr, sizeof(addr));
		if (rc < 0 && errno != EINPROGRESS)
		{
#ifdef DEBUG
			perror("connect");
#endif
			close(fd);
			continue;
		}

		BenchClient *client = calloc(1, sizeof(BenchClient));
		if (!client)
		{
			close(fd);
			continue;
		}

		client->fd = fd;
		client->state = (rc == 0) ? 1 : 0;
		client->message = message;
		client->msg_len = strlen(message);
		client->requests_planned = requests;
		client->requests_completed = 0;
		client->userdata = &stats;

		MuxHandler *handler = malloc(sizeof(MuxHandler));
		if (!handler)
		{
			free(client);
			close(fd);
			continue;
		}

		memset(handler, 0, sizeof(*handler));
		handler->userdata = client;
		handler->on_write = bench_client_on_write;
		handler->on_read = bench_client_on_read;
		handler->on_error = bench_client_on_error;
		handler->on_close = bench_client_on_close;

		int	events = MUX_EVENT_WRITE;
		if (rc == 0)
			events |= MUX_EVENT_READ;

		if (mux_add(mux, fd, events, handler) < 0)
		{
#ifdef DEBUG
			perror("mux_add");
#endif
			free(handler);
			free(client);
			close(fd);
			continue;
		}

		if (rc == 0)
			start_request(mux, client);
	}

	mux_run(mux, 1000);

	clock_gettime(CLOCK_MONOTONIC, &stats.bench_end);
#ifdef DEBUG
	double	total_time = (stats.bench_end.tv_sec - stats.bench_start.tv_sec) +
		(stats.bench_end.tv_nsec - stats.bench_start.tv_nsec) / 1000000000.0;
	printf("\nBenchmark results:\n");
	printf("  Clients:           %d\n", clients);
	printf("  Requests/client:   %d\n", requests);
	printf("  Total requests:    %d\n", stats.total_requests);
	printf("  Successful:        %d\n", stats.completed_requests);
	printf("  Failed:            %d\n", stats.failed_requests);
	printf("  Total time:        %.3f seconds\n", total_time);

	if (total_time > 0)
		printf("  Requests/sec:      %.2f\n", stats.completed_requests / total_time);
	else
		printf("  Requests/sec:      N/A\n");

	if (stats.completed_requests > 0)
		printf("  Avg latency:       %.3f ms\n", stats.total_latency / stats.completed_requests);
	else
		printf("  Avg latency:       N/A (no successful requests)\n");
#endif
	mux_destroy(mux);
	return 0;
}

/* ---------------- UDP Benchmark Implementation ---------------- */

/* UDP benchmark client context structure */
typedef struct {
	int	fd;
	struct sockaddr_in addr;
	const char *msg;
	size_t	msg_len;
	int	requests;
	int	completed;
	BenchStats	*stats;
	struct timespec	start;
	MuxHandler	*self_handler;
} UdpBenchClient;

/* Handle read events for UDP benchmark clients (track completed requests) */
static int
udp_bench_on_read(void *_mux __attribute__((unused)), int fd __attribute__((unused)), void *ud)
{
	UdpBenchClient	*c = (UdpBenchClient *) ud;
	char	buf[8192];
	struct sockaddr_in	src;
	socklen_t	slen = sizeof(src);
	ssize_t	n = recvfrom(fd, buf, sizeof(buf), 0, (struct sockaddr *) &src, &slen);

	if (n <= 0)
		return MUX_CB_OK;

	clock_gettime(CLOCK_MONOTONIC, &c->stats->bench_end);
	c->completed++;
	c->stats->completed_requests++;

	if (c->completed < c->requests)
	{
		clock_gettime(CLOCK_MONOTONIC, &c->start);
		sendto(fd, c->msg, c->msg_len, 0, (struct sockaddr *) &c->addr, sizeof(c->addr));
	}
	else
	{
		c->stats->completed_clients++;
		if (c->stats->completed_clients >= c->stats->total_clients)
			mux_stop(c->stats->mux);

		return MUX_CB_REMOVE;
	}

	return MUX_CB_OK;
}

/* Free a benchmark client's socket/handler/context on removal or shutdown. */
static int
udp_bench_on_close(void *_mux __attribute__((unused)), int fd __attribute__((unused)), void *ud)
{
	UdpBenchClient *c = (UdpBenchClient *) ud;
	if (c)
	{
		if (c->fd >= 0)
			close(c->fd);
		free(c->self_handler);
		free(c);
	}
	return MUX_CB_OK;
}

/* Run UDP benchmark with specified clients/requests/message */
int
run_udp_bench(const char *host, int port, int clients, int requests, const char *message)
{
	Mux	*mux = mux_create(MUX_BACKEND_EPOLL);

	if (!mux)
	{
		fprintf(stderr, "mux_create failed\n");
		return -1;
	}

	BenchStats stats = {
		.total_clients = clients,
		.completed_clients = 0,
		.total_requests = clients * requests,
		.completed_requests = 0,
		.failed_requests = 0,
		.mux = mux
	};

	clock_gettime(CLOCK_MONOTONIC, &stats.bench_start);

	for (int i = 0; i < clients; i++)
	{
		int	fd = socket(AF_INET, SOCK_DGRAM, 0);
		if (fd < 0)
			continue;

		set_nonblocking(fd, 1);

		struct sockaddr_in addr;
		memset(&addr, 0, sizeof(addr));
		addr.sin_family = AF_INET;
		addr.sin_port = htons(port);
		inet_pton(AF_INET, host, &addr.sin_addr);

		UdpBenchClient *client = calloc(1, sizeof(UdpBenchClient));
		if (!client) { close(fd); continue; }
		client->fd = fd;
		client->addr = addr;
		client->msg = message;
		client->msg_len = strlen(message);
		client->requests = requests;
		client->completed = 0;
		client->stats = &stats;

		MuxHandler *handler = calloc(1, sizeof(MuxHandler));
		if (!handler) { free(client); close(fd); continue; }
		handler->userdata = client;
		handler->on_read = udp_bench_on_read;
		handler->on_close = udp_bench_on_close;
		client->self_handler = handler;

		mux_add(mux, fd, MUX_EVENT_READ, handler);

		sendto(fd, message, strlen(message), 0, (struct sockaddr *) &addr, sizeof(addr));
		clock_gettime(CLOCK_MONOTONIC, &client->start);
	}

	mux_run(mux, 1000);

	clock_gettime(CLOCK_MONOTONIC, &stats.bench_end);
#ifdef DEBUG
	double	total_time = (stats.bench_end.tv_sec - stats.bench_start.tv_sec) +
		(stats.bench_end.tv_nsec - stats.bench_start.tv_nsec) / 1e9;
	printf("\nUDP Benchmark results:\n");
	printf("  Clients:           %d\n", clients);
	printf("  Requests/client:   %d\n", requests);
	printf("  Total requests:    %d\n", stats.total_requests);
	printf("  Completed:         %d\n", stats.completed_requests);
	printf("  Total time:        %.3f sec\n", total_time);

	if (total_time > 0)
		printf("  Requests/sec:      %.2f\n", stats.completed_requests / total_time);
#endif
	mux_destroy(mux);
	return 0;
}

/* ---------------- Non-blocking I/O Test Implementation ---------------- */

/* Non-blocking test server info structure */
typedef struct {
	int	port;
	pthread_t	thread;
} NBTestServerInfo;

/* Non-blocking test client context structure */
typedef struct {
	int	fd;
	const char	*msg;
	size_t	msg_len;
	size_t	sent;
	size_t	received;
	char	buffer[1024];
	int	done;
	Mux	*mux;
} NBTestClient;

/* Handle write events for non-blocking test client (send test message) */
static int
nb_client_on_write(void *_mux, int fd, void *ud)
{
	Mux	*mux = (Mux *) _mux;
	NBTestClient *c = (NBTestClient *) ud;

	if (c->sent < c->msg_len)
	{
		ssize_t	n = send(fd, c->msg + c->sent, c->msg_len - c->sent, 0);

		if (n > 0)
		{
			c->sent += n;
		}
		else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
		{
			return MUX_CB_OK;
		}
		else if (n < 0 && errno != EINTR)
		{
			return MUX_CB_REMOVE;
		}
	}

	if (c->sent >= c->msg_len)
	{
		mux_mod(mux, fd, MUX_EVENT_READ);
	}

	return MUX_CB_OK;
}

/* Handle read events for non-blocking test client (verify response) */
static int
nb_client_on_read(void *_mux, int fd, void *ud)
{
	Mux	*mux = (Mux *) _mux;
	NBTestClient *c = (NBTestClient *) ud;

	while (1)
	{
		ssize_t	n = recv(fd, c->buffer + c->received,
							 sizeof(c->buffer) - 1 - c->received, 0);

		if (n > 0)
		{
			c->received += n;

			if (c->received >= c->msg_len)
			{
				c->buffer[c->received] = '\0';
#ifdef DEBUG
				if (strncmp(c->buffer, c->msg, c->msg_len) == 0)
				{
					printf("Non-blocking I/O test passed: %s\n", c->buffer);
				}
				else
				{
					printf("Mismatch: got [%s]\n", c->buffer);
				}
#endif
				c->done = 1;
				mux_stop(mux);
				return MUX_CB_REMOVE;
			}
		}
		else if (n == 0)
		{
			c->done = 1;
			mux_stop(mux);
			return MUX_CB_REMOVE;
		}
		else if (errno == EAGAIN || errno == EWOULDBLOCK)
		{
			break;
		}
		else if (errno == EINTR)
		{
			continue;
		}
		else
		{
			mux_stop(mux);
			return MUX_CB_REMOVE;
		}
	}

	return MUX_CB_OK;
}

/* Handle error events for non-blocking test client (report errors) */
static int
nb_client_on_error(void *_mux __attribute__((unused)), int fd __attribute__((unused)), void *ud __attribute__((unused)))
{
	return MUX_CB_REMOVE;
}

/* Run non-blocking I/O test (internal server + client) */
/*
 * Test server thread entry. Defined at file scope rather than as a GCC nested
 * function so the module also compiles under Clang (which does not support the
 * nested-function extension).
 */
static void *
nbtest_server_thread(void *arg)
{
	NBTestServerInfo *info = (NBTestServerInfo *) arg;
	run_echo_server(info->port, MUX_BACKEND_EPOLL);
	return NULL;
}

int
run_nbtest(void)
{
	/* Start test server in background thread */
	NBTestServerInfo srv = { .port = 9876 };

	if (pthread_create(&srv.thread, NULL, nbtest_server_thread, &srv) != 0)
		return -1;
	usleep(100000);/* Wait for server to start */

	/* Create test client */
	Mux	*mux = mux_create(MUX_BACKEND_EPOLL);
	if (!mux)
		return -1;

	int	fd = socket(AF_INET, SOCK_STREAM, 0);
	if (fd < 0)
	{
		mux_destroy(mux);
		return -1;
	}

	set_nonblocking(fd, 1);

	struct sockaddr_in addr;
	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_port = htons(srv.port);
	inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

	int	rc = connect(fd, (struct sockaddr *) &addr, sizeof(addr));
	if (rc < 0 && errno != EINPROGRESS)
	{
		close(fd);
		mux_destroy(mux);
		return -1;
	}

	const char *test_msg = "Non-blocking I/O test message";
	NBTestClient *client = calloc(1, sizeof(NBTestClient));
	if (!client)
	{
		close(fd);
		mux_destroy(mux);
		return -1;
	}
	client->fd = fd;
	client->msg = test_msg;
	client->msg_len = strlen(test_msg);
	client->mux = mux;

	MuxHandler *handler = calloc(1, sizeof(MuxHandler));
	if (!handler)
	{
		free(client);
		close(fd);
		mux_destroy(mux);
		return -1;
	}
	handler->userdata = client;
	handler->on_write = nb_client_on_write;
	handler->on_read = nb_client_on_read;
	handler->on_error = nb_client_on_error;

	int	events = MUX_EVENT_WRITE;
	if (rc == 0)
		events |= MUX_EVENT_READ;

	mux_add(mux, fd, events, handler);
	mux_run(mux, 5000);

	/* Cleanup */
	mux_destroy(mux);
	close(fd);
	free(handler);
	free(client);
	pthread_cancel(srv.thread);
	pthread_join(srv.thread, NULL);

	return 0;
}
