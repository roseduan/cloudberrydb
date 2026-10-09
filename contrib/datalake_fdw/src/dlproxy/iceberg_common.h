#ifndef ICEBERG_COMMON_H
#define ICEBERG_COMMON_H

#include "postgres.h"

/*
 * Common function declarations for iceberg-related operations
 */

/* Function to parse fragment response from iceberg service */
List *parseIcebergFragmentResponse(char *buffer, size_t buffer_size);
char *extractCatalogPropertiesEnvelope(const char *buffer, size_t buffer_size);

#endif /* ICEBERG_COMMON_H */
