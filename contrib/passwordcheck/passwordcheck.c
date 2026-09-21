/*-------------------------------------------------------------------------
 *
 * passwordcheck.c
 *
 *
 * Copyright (c) 2009-2023, PostgreSQL Global Development Group
 *
 * Author: Laurenz Albe <laurenz.albe@wien.gv.at>
 *
 * IDENTIFICATION
 *	  contrib/passwordcheck/passwordcheck.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>

#ifdef USE_CRACKLIB
#include <crack.h>
#endif

#include "commands/user.h"
#include "fmgr.h"
#include "libpq/crypt.h"
#include "utils/guc.h"

PG_MODULE_MAGIC;

/* Saved hook value in case of unload */
static check_password_hook_type prev_check_password_hook = NULL;

/* GUC variable */
static bool passwordcheck_strict_policy = false;

/* passwords shorter than this will be rejected */
#define MIN_PWD_LENGTH			8

/* strict policy thresholds */
#define STRICT_MIN_PWD_LENGTH	9
#define STRICT_MIN_UPPERCASE	2
#define STRICT_MIN_LOWERCASE	2
#define STRICT_MIN_DIGITS		2
#define STRICT_MIN_SPECIAL		2

/*
 * check_password
 *
 * performs checks on an encrypted or unencrypted password
 * ereport's if not acceptable
 *
 * username: name of role being created or changed
 * password: new password (possibly already encrypted)
 * password_type: PASSWORD_TYPE_* code, to indicate if the password is
 *			in plaintext or encrypted form.
 * validuntil_time: password expiration time, as a timestamptz Datum
 * validuntil_null: true if password expiration time is NULL
 *
 * This sample implementation doesn't pay any attention to the password
 * expiration time, but you might wish to insist that it be non-null and
 * not too far in the future.
 */
static void
check_password(const char *username,
			   const char *shadow_pass,
			   PasswordType password_type,
			   Datum validuntil_time,
			   bool validuntil_null)
{
	if (prev_check_password_hook)
		prev_check_password_hook(username, shadow_pass,
								 password_type, validuntil_time,
								 validuntil_null);

	if (password_type != PASSWORD_TYPE_PLAINTEXT)
	{
		/*
		 * Unfortunately we cannot perform exhaustive checks on encrypted
		 * passwords - we are restricted to guessing. (Alternatively, we could
		 * insist on the password being presented non-encrypted, but that has
		 * its own security disadvantages.)
		 *
		 * We only check for username = password.
		 */
		const char *logdetail = NULL;

		if (plain_crypt_verify(username, shadow_pass, username, &logdetail) == STATUS_OK)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("password must not equal user name")));
	}
	else
	{
		/*
		 * For unencrypted passwords we can perform better checks
		 */
		const char *password = shadow_pass;
		int			pwdlen = strlen(password);
		int			i;
#ifdef USE_CRACKLIB
		const char *reason;
#endif

		if (passwordcheck_strict_policy)
		{
			/*
			 * Strict policy: enforce minimum length, character type counts,
			 * and username substring check.
			 */
			int			pwd_upper = 0;
			int			pwd_lower = 0;
			int			pwd_digit = 0;
			int			pwd_special = 0;

			if (pwdlen < STRICT_MIN_PWD_LENGTH)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("password is too short, minimum is %d characters",
								STRICT_MIN_PWD_LENGTH)));

			/* check if the password contains the username */
			if (strstr(password, username))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("password must not contain user name")));

			/* count character types */
			for (i = 0; i < pwdlen; i++)
			{
				unsigned char	c = (unsigned char) password[i];

				if (isupper(c))
					pwd_upper++;
				else if (islower(c))
					pwd_lower++;
				else if (isdigit(c))
					pwd_digit++;
				else
					pwd_special++;
			}

			if (pwd_upper < STRICT_MIN_UPPERCASE)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("password must contain at least %d uppercase letters",
								STRICT_MIN_UPPERCASE)));

			if (pwd_lower < STRICT_MIN_LOWERCASE)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("password must contain at least %d lowercase letters",
								STRICT_MIN_LOWERCASE)));

			if (pwd_digit < STRICT_MIN_DIGITS)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("password must contain at least %d digits",
								STRICT_MIN_DIGITS)));

			if (pwd_special < STRICT_MIN_SPECIAL)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("password must contain at least %d special characters",
								STRICT_MIN_SPECIAL)));
		}
		else
		{
			/*
			 * Default policy: enforce minimum length, username substring
			 * check, and require both letters and non-letters.
			 */
			bool		pwd_has_letter = false;
			bool		pwd_has_nonletter = false;

			if (pwdlen < MIN_PWD_LENGTH)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("password is too short")));

			/* check if the password contains the username */
			if (strstr(password, username))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("password must not contain user name")));

			/*
			 * check if the password contains both letters and non-letters
			 *
			 * isalpha() does not work for multibyte encodings but let's
			 * consider non-ASCII characters non-letters
			 */
			for (i = 0; i < pwdlen; i++)
			{
				if (isalpha((unsigned char) password[i]))
					pwd_has_letter = true;
				else
					pwd_has_nonletter = true;
			}
			if (!pwd_has_letter || !pwd_has_nonletter)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("password must contain both letters and nonletters")));
		}

#ifdef USE_CRACKLIB
		/* call cracklib to check password */
		if ((reason = FascistCheck(password, CRACKLIB_DICTPATH)))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("password is easily cracked"),
					 errdetail_log("cracklib diagnostic: %s", reason)));
#endif
	}

	/* all checks passed, password is ok */
}

/*
 * Module initialization function
 */
void
_PG_init(void)
{
	DefineCustomBoolVariable("passwordcheck.strict_policy",
							 "Enforce strict password policy requiring uppercase, lowercase, digits, and special characters.",
							 NULL,
							 &passwordcheck_strict_policy,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	/* activate password checks when the module is loaded */
	prev_check_password_hook = check_password_hook;
	check_password_hook = check_password;
}
