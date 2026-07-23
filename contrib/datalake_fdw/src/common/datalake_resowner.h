/*-------------------------------------------------------------------------
 *
 * datalake_resowner.h
 *    ResourceOwner-based cleanup registry for datalake_fdw contexts.
 *
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 *
 * IDENTIFICATION
 *        contrib/datalake_fdw/src/common/datalake_resowner.h
 *-------------------------------------------------------------------------
 */
 #ifndef DATALAKE_RESOWNER_H
 #define DATALAKE_RESOWNER_H


 #include "postgres.h"
 #include "utils/resowner.h"

 typedef struct datalake_context_handle_t
 {
     int cid;
     bool gp_is_writer;
     ResourceOwner owner;	/* owner of this handle */
     struct datalake_context_handle_t *next;
     struct datalake_context_handle_t *prev;
 } datalake_context_handle_t;

 datalake_context_handle_t* datalake_register_resource_context(bool gp_is_writer);

 void cleanup_datalake_resource_context(datalake_context_handle_t* h);

 #endif /* DATALAKE_RESOWNER_H */
