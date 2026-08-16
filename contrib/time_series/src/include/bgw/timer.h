/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 *
 * Portions Copyright (c) 2025-2026, HashData Technology Limited.
 */
#ifndef BGW_TIMER_H
#define BGW_TIMER_H

#include <postgres.h>
#include <utils/timestamp.h>

typedef struct Timer
{
	TimestampTz (*get_current_timestamp)();
	bool (*wait)(TimestampTz until);
} Timer;

extern bool timer_wait(TimestampTz until);
extern TimestampTz timer_get_current_timestamp(void);

extern void timer_set(const Timer *timer);
extern const Timer *get_standard_timer(void);

#endif /* BGW_TIMER_H */
