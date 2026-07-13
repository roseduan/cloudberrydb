# datalake_rest_catalog — security deployment (sub-project C)

Closes the privilege-escalation chain reproduced in issue #382. **Required for production / shared
environments**, in this order:

1. Create a dedicated authenticator (non-superuser):
   `psql -p 7000 postgres -v pw="<strong-secret>" -f security/create_authenticator.sql`
   (Note: pass the raw password to `-v pw=`; do NOT wrap it in extra quotes yourself — the SQL's
   `:'pw'` already quotes it into a literal, and an extra layer would store the literal quote
   characters as part of the password.)
   `create_authenticator.sql`'s `IF NOT EXISTS` only creates the role when it does not exist; it does
   **not** rotate the password of an existing role. To rotate the secret, run
   `ALTER ROLE iceberg_authenticator PASSWORD '<new-secret>';` separately.
2. Apply the hardened pg_hba: merge the contents of `pg_hba.rest_catalog.fragment` into the
   coordinator's `pg_hba.conf`. pg_hba is **first-match-wins**, so place it **above** any broad
   catch-all line that could match the gateway's source — not only `trust`, but also generic lines
   like `host all all <cidr> md5/password/scram` (e.g. a leftover e2e `host all all 127.0.0.1/32 md5`).
   If a broader same-source line comes first, it shadows the scram line below and the hardening
   silently fails. After editing, reload with `gpstop -u`.
3. Configure the gateway with `pg.authenticator.user=iceberg_authenticator` and supply its password
   via env.
4. Start the gateway: `AuthPreflight` **refuses to start** when pg_hba is still trust or the
   authenticator is a superuser.
5. (If Task 3 is done) apply `security/rest_catalog_authz.sql` to remove the direct-DB pg_ext_aux leak.

## Self-check
- A wrong password must be rejected:
  `PGPASSWORD=wrong psql -h 127.0.0.1 -p 7000 -U iceberg_authenticator postgres`
  Expected `FATAL: password authentication failed` (if it connects, trust is still in effect and the
  hardening was not applied).
- gpadmin exchanging a token via the gateway must get 401 (Task 1 D2).
