#-------------------------------------------------------------------------
#
# Makefile
#    Makefile for src/backend/crypto
#
# IDENTIFICATION
#    src/backend/crypto/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/crypto
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	bufenc.o \
	kms_builtin.o \
	kms_cosmian.o \
	kms_kmip.o \
	kms_local_cmd.o \
	kmgr.o \
	sm4_ctr.o \
	sm4_ofb.o \
	tblspc_enc.o \
	tblspc_kmgr.o

include $(top_srcdir)/src/backend/common.mk
